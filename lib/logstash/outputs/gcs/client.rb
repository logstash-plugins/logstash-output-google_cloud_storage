require 'thread'
require 'java'
require 'logstash-output-google_cloud_storage_jars.rb'

module LogStash
  module Outputs
    module Gcs
      class Client
        def initialize(bucket, json_key_path, logger)
          @logger = logger
          @bucket = bucket

          # create client
          @storage = initialize_storage json_key_path
        end

        def upload_object(file_path, content_encoding, content_type)
          input = java.io.FileInputStream.new(file_path)

          blob_name = ::File.basename(file_path)
          blob_info = com.google.cloud.storage.BlobInfo.newBuilder(@bucket, blob_name)
                          .setContentEncoding(content_encoding)
                          .setContentType(content_type)
                          .build

          @logger.info("Uploading file to #{@bucket}/#{blob_name}")
          @storage.create(blob_info, input)

          input.close
          @logger.info("Uploaded file to #{@bucket}/#{blob_name}")
        end

        def initialize_storage(json_key_path)
          @logger.info("Initializing Google API client, key: #{json_key_path}")

          com.google.cloud.storage.StorageOptions.newBuilder()
              .setCredentials(credentials(json_key_path))
              .setHeaderProvider(http_headers)
              .setRetrySettings(retry_settings)
              .build()
              .getService()
        end

        private

        java_import 'com.google.auth.oauth2.GoogleCredentials'
        def credentials(json_key_path)
          return GoogleCredentials.getApplicationDefault() if nil_or_empty?(json_key_path)

          key_file = java.io.FileInputStream.new(json_key_path)
          GoogleCredentials.fromStream(key_file)
        end

        java_import 'com.google.api.gax.rpc.FixedHeaderProvider'
        def http_headers
          gem_name = 'logstash-output-google_cloud_storage'
          gem_version = '4.0.0'
          user_agent = "Elastic/#{gem_name} version/#{gem_version}"

          FixedHeaderProvider.create({ 'User-Agent' => user_agent })
        end

        java_import 'com.google.api.gax.retrying.RetrySettings'
        java_import 'org.threeten.bp.Duration'
        def retry_settings
          # backoff values taken from com.google.api.client.util.ExponentialBackOff
          RetrySettings.newBuilder()
              .setInitialRetryDelay(Duration.ofMillis(500))
              .setRetryDelayMultiplier(1.5)
              .setMaxRetryDelay(Duration.ofSeconds(60))
              .setInitialRpcTimeout(Duration.ofSeconds(20))
              .setRpcTimeoutMultiplier(1.5)
              .setMaxRpcTimeout(Duration.ofSeconds(20))
              .setTotalTimeout(Duration.ofMinutes(15))
              .build()
        end

        def api_debug(message, dataset, table)
          @logger.debug(message, dataset: dataset, table: table)
        end

        def nil_or_empty?(param)
          param.nil? || param.empty?
        end
      end
    end
  end
end