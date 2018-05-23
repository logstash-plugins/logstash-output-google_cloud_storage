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
require "logstash/outputs/base"
require "logstash/outputs/gcs/path_factory"
require "logstash/outputs/gcs/worker_pool"
require "logstash/namespace"
require "logstash/json"
require "stud/interval"
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
#      key_path => "/path/to/privatekey.p12"                     (required)
#      key_password => "notasecret"                              (optional)
#      service_account => "1234@developer.gserviceaccount.com"   (required)
#      temp_directory => "/tmp/logstash-gcs"                     (optional)
#      log_file_prefix => "logstash_gcs"                         (optional)
#      max_file_size_kbytes => 1024                              (optional)
#      output_format => "plain"                                  (optional)
#      date_pattern => "%Y-%m-%dT%H:00"                          (optional)
#      flush_interval_secs => 2                                  (optional)
#      gzip => false                                             (optional)
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

  # GCS bucket name, without "gs://" or any other prefix.
  config :bucket, :validate => :string, :required => true

  # GCS path to private key file.
  config :key_path, :validate => :string, :required => true

  # GCS private key password.
  config :key_password, :validate => :string, :default => "notasecret"

  # GCS service account.
  config :service_account, :validate => :string, :required => true

  # Directory where temporary files are stored.
  # Defaults to /tmp/logstash-gcs-<random-suffix>
  config :temp_directory, :validate => :string, :default => ""

  # Log file prefix. Log file will follow the format:
  # <prefix>_hostname_date<.part?>.log
  config :log_file_prefix, :validate => :string, :default => "logstash_gcs"

  # Sets max file size in kbytes. 0 disable max file check.
  config :max_file_size_kbytes, :validate => :number, :default => 10000

  # The event format you want to store in files. Defaults to plain text.
  config :output_format, :validate => [ "json", "plain" ], :default => "plain"

  # Time pattern for log file, defaults to hourly files.
  # Must Time.strftime patterns: www.ruby-doc.org/core-2.0/Time.html#method-i-strftime
  config :date_pattern, :validate => :string, :default => "%Y-%m-%dT%H:00"

  # Flush interval in seconds for flushing writes to log files. 0 will flush
  # on every message.
  config :flush_interval_secs, :validate => :number, :default => 2

  # Gzip output stream when writing events to log files.
  config :gzip, :validate => :boolean, :default => false

  # Uploader interval when uploading new files to GCS. Adjust time based
  # on your time pattern (for example, for hourly files, this interval can be
  # around one hour).
  config :uploader_interval_secs, :validate => :number, :default => 60

  # Should the hostname be included in the file name?
  config :include_hostname, :validate => :boolean, :default => true

  # Should a UUID be included in the file name?
  config :include_uuid, :validate => :boolean, :default => false

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

  def register
    require "fileutils"
    require "thread"
    @logger.debug("GCS: register plugin")
    @last_flush_cycle = Time.now

    @workers = LogStash::Outputs::Gcs::WorkerPool.new(@max_concurrent_uploads, @upload_synchronous)
    initialize_temp_directory
    initialize_path_factory
    open_current_file

    initialize_google_client

    start_uploader

    if @gzip
      @content_type = 'application/gzip'
    else
      @content_type = 'text/plain'
    end
  end

  # Method called for each log event. It writes the event to the current output
  # file, flushing depending on flush interval configuration.
  def receive(event)
    @logger.debug("GCS: receive method called", :event => event)

    if (@output_format == "json")
      message = LogStash::Json.dump(event.to_hash)
    else
      message = event.to_s
    end

    # Time to roll file based on the date pattern? Or is it over the size limit?
    initialize_next_log if ready_to_rotate?

    @temp_file.write(message)
    @temp_file.write("\n")

    sync_log_file()

    @logger.debug("GCS: event appended to log file",
                  :filename => File.basename(@temp_file.to_path))
  end

  def close
    @logger.debug('Stopping the plugin, uploading the remaining files.')

    Stud.stop!(@registration_thread) unless @registration_thread.nil?

    close_and_upload_current
    @workers.stop!
  end

  private


  def ready_to_rotate?
    path_changed = @path_factory.should_rotate?
    too_big = @max_file_size_kbytes > 0 && @temp_file.size >= @max_file_size_kbytes * 1024

    path_changed || too_big
  end

  ##
  # Flushes temporary log file every flush_interval_secs seconds or so.
  # This is triggered by events, but if there are no events there's no point
  # flushing files anyway.
  #
  # Inspired by lib/logstash/outputs/file.rb (flush(fd), flush_pending_files)
  def sync_log_file
    if flush_interval_secs <= 0
      @temp_file.fsync()
      return
    end

    return unless Time.now - @last_flush_cycle >= flush_interval_secs
    @temp_file.fsync()
    @logger.debug("GCS: flushing file",
                  :path => @temp_file.to_path,
                  :fd => @temp_file)
    @last_flush_cycle = Time.now
  end

  ##
  # Creates temporary directory, if it does not exist.
  #
  # A random suffix is appended to the temporary directory
  def initialize_temp_directory
    require "stud/temporary"
    if @temp_directory.empty?
      @temp_directory = Stud::Temporary.directory("logstash-gcs")
      @logger.info("GCS: temporary directory generated",
                   :directory => @temp_directory)
    end

    if !(File.directory? @temp_directory)
      @logger.debug("GCS: directory doesn't exist. Creating it.",
                    :directory => @temp_directory)
      FileUtils.mkdir_p(@temp_directory)
    end
  end

  def initialize_path_factory
    @path_factory = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
      builder.set_directory @temp_directory
      builder.set_prefix @log_file_prefix
      builder.set_include_host @include_hostname
      builder.set_date_pattern @date_pattern
      builder.set_include_part(@max_file_size_kbytes > 0)
      builder.set_include_uuid @include_uuid
      builder.set_is_gzipped @gzip
    end
  end

  def start_uploader
    Thread.new do
      @registration_thread = Thread.current
      Stud.interval(@uploader_interval_secs) do
        initialize_next_log if ready_to_rotate?
      end
    end
  end

  ##
  # Opens current log file and updates @temp_file with an instance of IOWriter.
  # This method also adds file to the upload queue.
  def open_current_file
    path = @path_factory.current_path

    fd = File.new(path, "a")
    fd = Zlib::GzipWriter.new(fd) if @gzip

    @temp_file = GCSIOWriter.new(fd)
  end

  ##
  # Generates new log file name based on configuration options and opens log
  # file. If max file size is enabled, part number if incremented in case the
  # the base log file name is the same (e.g. log file was not rolled given the
  # date pattern).
  def initialize_next_log
    close_and_upload_current
    @path_factory.rotate_path!
    open_current_file
  end

  ##
  # Initializes Google Client instantiating client and authorizing access.
  def initialize_google_client
    require "google/api_client"
    require "openssl"

    @client = Google::APIClient.new(:application_name =>
                                    'Logstash Google Cloud Storage output plugin',
                                    :application_version => '0.1')
    @storage = @client.discovered_api('storage', 'v1')

    key = Google::APIClient::PKCS12.load_key(@key_path, @key_password)
    service_account = Google::APIClient::JWTAsserter.new(@service_account,
                                                         'https://www.googleapis.com/auth/devstorage.read_write',
                                                         key)
    @client.authorization = service_account.authorize
  end

  ##
  # Uploads a local file to the configured bucket.
  def upload_object(filename)
    begin
      @logger.debug("GCS: upload object.", :filename => filename)

      media = Google::APIClient::UploadIO.new(filename, @content_type)
      metadata_insert_result = @client.execute(:api_method => @storage.objects.insert,
                                               :parameters => {
                                                 'uploadType' => 'multipart',
                                                 'bucket' => @bucket,
                                                 'name' => File.basename(filename)
                                               },
                                               :body_object => {contentType: @content_type},
                                               :media => media)
      contents = metadata_insert_result.data
      @logger.debug("GCS: multipart insert",
                    :object => contents.name,
                    :self_link => contents.self_link)
    rescue => e
      @logger.error("GCS: failed to upload file", :exception => e)
      # TODO(rdc): limit retries?
      sleep 1
      retry
    end
  end

  def close_and_upload_current
    return if @temp_file.nil?

    filename = @temp_file.to_path
    @temp_file.fsync
    @temp_file.close
    @logger.info("Uploading file: #{filename}")

    @workers.post do
      upload_and_delete(filename)
    end
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
end

##
# Wrapper class that abstracts which IO being used (for instance, regular
# files or GzipWriter.
#
# Inspired by lib/logstash/outputs/file.rb.
class GCSIOWriter
  def initialize(io)
    @io = io
  end
  def write(*args)
    @io.write(*args)
  end
  def fsync
    if @io.class == Zlib::GzipWriter
      @io.flush
      @io.to_io.fsync
    else
      @io.fsync
    end
  end
  def method_missing(method_name, *args, &block)
    if @io.respond_to?(method_name)
      @io.send(method_name, *args, &block)
    else
      if @io.class == Zlib::GzipWriter && @io.to_io.respond_to?(method_name)
        @io.to_io.send(method_name, *args, &block)
      else
        super
      end
    end
  end
  attr_accessor :active
end
