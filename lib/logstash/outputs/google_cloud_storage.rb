# [source,txt]
# -----
# encoding: utf-8
# Author: Rodrigo De Castro <rdc@google.com>
# Date: 2013-09-20
#
# Copyright 2013 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# -----
require 'logstash/outputs/gcs/client'
require "logstash/outputs/base"
require "logstash/outputs/gcs/path_factory"
require "logstash/outputs/gcs/worker_pool"
require "logstash/outputs/gcs/log_rotate"
require "logstash/namespace"
require "logstash/json"
require "stud/interval"
require "thread"
require "zlib"

# Summary: plugin to upload log events to Google Cloud Storage (GCS), rolling
# files based on the date pattern provided as a configuration setting. Events
# are written to files locally and, once file is closed, this plugin uploads
# it to the configured bucket.
#
# For more info on Google Cloud Storage, please go to:
# https://cloud.google.com/products/cloud-storage
#
# In order to use this plugin, a Google service account must be used. For
# more information, please refer to:
# https://developers.google.com/storage/docs/authentication#service_accounts
#
# Recommendation: experiment with the settings depending on how much log
# data you generate, so the uploader can keep up with the generated logs.
# Using gzip output can be a good option to reduce network traffic when
# uploading the log files and in terms of storage costs as well.
#
# USAGE:
# This is an example of logstash config:
#
# [source,json]
# --------------------------
# output {
#    google_cloud_storage {
#      bucket => "my_bucket"                                     (required)
#      json_key_file => "/path/to/privatekey.json"               (optional)
#      temp_directory => "/tmp/logstash-gcs"                     (optional)
#      log_file_prefix => "logstash_gcs"                         (optional)
#      max_file_size_kbytes => 1024                              (optional)
#      date_pattern => "%Y-%m-%dT%H:00"                          (optional)
#      flush_interval_secs => 2                                  (optional)
#      gzip => false                                             (optional)
#      gzip_content_encoding => false                            (optional)
#      uploader_interval_secs => 60                              (optional)
#      upload_synchronous => false                               (optional)
#    }
# }
# --------------------------
#
# Improvements TODO list:
# * Support logstash event variables to determine filename.
# * Turn Google API code into a Plugin Mixin (like AwsConfig).
# * There's no recover method, so if logstash/plugin crashes, files may not
# be uploaded to GCS.
# * Allow user to configure file name.
class LogStash::Outputs::GoogleCloudStorage < LogStash::Outputs::Base
  config_name "google_cloud_storage"

  concurrency :single
  default :codec, "line"

  # GCS bucket name, without "gs://" or any other prefix.
  config :bucket, :validate => :string, :required => true

  # GCS path to private key file.
  config :key_path, :validate => :string, :obsolete => 'Use json_key_file or ADC instead.'

  # GCS private key password.
  config :key_password, :validate => :string, :deprecated => 'Use json_key_file or ADC instead.'

  # GCS service account.
  config :service_account, :validate => :string, :deprecated => 'Use json_key_file or ADC instead.'

  # Directory where temporary files are stored.
  # Defaults to /tmp/logstash-gcs-<random-suffix>
  config :temp_directory, :validate => :string, :default => ""

  # Log file prefix. Log file will follow the format:
  # <prefix>_hostname_date<.part?>.log
  config :log_file_prefix, :validate => :string, :default => "logstash_gcs"

  # Sets max file size in kbytes. 0 disable max file check.
  config :max_file_size_kbytes, :validate => :number, :default => 10000

  # The event format you want to store in files. Defaults to plain text.
  config :output_format, :validate => [ "json", "plain", "" ], :default => "", :deprecated => 'Use codec instead.'

  # Time pattern for log file, defaults to hourly files.
  # Must Time.strftime patterns: www.ruby-doc.org/core-2.0/Time.html#method-i-strftime
  config :date_pattern, :validate => :string, :default => "%Y-%m-%dT%H:00"

  # Flush interval in seconds for flushing writes to log files. 0 will flush
  # on every message.
  config :flush_interval_secs, :validate => :number, :default => 2

  # Gzip output stream when writing events to log files, set
  # `Content-Type` to `application/gzip` instead of `text/plain`, and
  # use file suffix `.log.gz` instead of `.log`.
  config :gzip, :validate => :boolean, :default => false

  # Gzip output stream when writing events to log files and set
  # `Content-Encoding` to `gzip`.
  config :gzip_content_encoding, :validate => :boolean, :default => false

  # Uploader interval when uploading new files to GCS. Adjust time based
  # on your time pattern (for example, for hourly files, this interval can be
  # around one hour).
  config :uploader_interval_secs, :validate => :number, :default => 60

  # Should the hostname be included in the file name?
  config :include_hostname, :validate => :boolean, :default => true

  # Should a UUID be included in the file name?
  config :include_uuid, :validate => :boolean, :default => false

  # The path to the service account's JSON credentials file.
  # Application Default Credentials (ADC) are used if the path is blank.
  # See: https://cloud.google.com/docs/authentication/production
  #
  # You must run on GCP for ADC to work.
  config :json_key_file, :validate => :string, :default => ""

  # When true, files are uploaded by the event processing thread as soon as a file is ready.
  # When false, (the default behaviour), files will be uploaded in a dedicated thread.
  #
  # Enabling this option provides greater likelihood that all generated files will be
  # to GCS, especially in the event of a graceful shutdown of logstash, such as when an
  # input plugin reaches the end of events. This comes at the price of introducing delays
  # in the event processing pipeline as files are uploaded.
  #
  # When this feature is enabled, the uploader_interval_secs option has no effect.
  config :upload_synchronous, :validate => :boolean, :default => false

  config :max_concurrent_uploads, :validate  => :number, :default => 5

  attr_accessor :disable_uploader

  public
  def register
    @logger.debug('Registering Google Cloud Storage plugin')

    # NOTE: this is a hacky solution to get around the fact that we used to
    # do our own pseudo-codec processing. This should be removed in the
    # next major release.
    params['codec'] = LogStash::Plugin.lookup('codec', 'json_lines').new if @output_format == 'json'
    params['codec'] = LogStash::Plugin.lookup('codec', 'plain').new if @output_format == 'line'

    @workers = LogStash::Outputs::Gcs::WorkerPool.new(@max_concurrent_uploads, @upload_synchronous)
    initialize_temp_directory
    initialize_path_factory
    initialize_log_rotater

    initialize_google_client

    start_uploader

    @content_type = @gzip ? 'application/gzip' : 'text/plain'
    @content_encoding = @gzip_content_encoding ? 'gzip' : 'identity'
  end

  # Method called for incoming log events. It writes the event to the current output
  # file, flushing depending on flush interval configuration.
  public
  def multi_receive_encoded(event_encoded_pairs)
    encoded = event_encoded_pairs.map{ |event, encoded| encoded }
    @logger.debug? && @logger.debug('Received events', :events => encoded)

    @log_rotater.write(*encoded)
  end

  public
  def close
    @logger.debug('Stopping the plugin, uploading the remaining files.')
    Stud.stop!(@uploader_thread) unless @uploader_thread.nil?

    # Force rotate the log. If it contains data it will be submitted
    # to the work pool and will be uploaded before the plugin stops.
    @log_rotater.rotate_log!
    @workers.stop!
  end

  private

  ##
  # Creates temporary directory, if it does not exist.
  #
  # A random suffix is appended to the temporary directory
  def initialize_temp_directory
    require "stud/temporary"

    if @temp_directory.empty?
      @temp_directory = Stud::Temporary.directory('logstash-gcs')
    end

    FileUtils.mkdir_p(@temp_directory) unless File.directory?(@temp_directory)

    @logger.info("Using temporary directory: #{@temp_directory}")
  end

  def initialize_path_factory
    @path_factory = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
      builder.set_directory(@temp_directory)
      builder.set_prefix(@log_file_prefix)
      builder.set_include_host(@include_hostname)
      builder.set_date_pattern(@date_pattern)
      builder.set_include_part(@max_file_size_kbytes > 0)
      builder.set_include_uuid(@include_uuid)
      builder.set_is_gzipped(@gzip)
    end
  end

  # start_uploader periodically sends flush events through the log rotater
  def start_uploader
    return if @disable_uploader

    @uploader_thread = Thread.new do
      Stud.interval(@uploader_interval_secs) do
        @log_rotater.write
      end
    end
  end

  ##
  # Initializes Google Client instantiating client and authorizing access.
  def initialize_google_client
    @client = LogStash::Outputs::Gcs::Client.new(@bucket, @json_key_file, @logger)
  end

  ##
  # Uploads a local file to the configured bucket.
  def upload_object(filename)
    @client.upload_object(filename, @content_encoding, @content_type)
  end

  def upload_and_delete(filename)
    file_size = File.stat(filename).size

    if file_size > 0
      upload_object(filename)
    else
      @logger.debug('File size is zero, skip upload.', :filename => filename)
    end

    @logger.debug('Delete local temporary file', :filename => filename)
    File.delete(filename)
  end

  def initialize_log_rotater
    max_file_size = @max_file_size_kbytes * 1024
    @log_rotater = LogStash::Outputs::Gcs::LogRotate.new(@path_factory, max_file_size, @gzip, @flush_interval_secs, @gzip_content_encoding)

    @log_rotater.on_rotate do |filename|
      @logger.info("Rotated out file: #{filename}")
      @workers.post do
        upload_and_delete(filename)
      end
    end
  end
  attr_accessor :active
end
