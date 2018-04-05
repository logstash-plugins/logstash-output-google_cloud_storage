require 'thread'
require 'java'
require 'logstash-output-google_cloud_storage_jars.rb'

module LogStash
  module Outputs
    module Gcs
      class Client
        def initialize bucket, json_key_path, logger
          @logger = logger
          @bucket = bucket

          # create client
          @storage = initialize_storage json_key_path
        end

        def upload_object file_path
          begin
            input = java.io.FileInputStream.new file_path

            blob_name = ::File.basename(file_path)
            blob_info = com.google.cloud.storage.BlobInfo.newBuilder(@bucket, blob_name).build()

            @logger.info("Uploading file to #{@bucket}/#{blob_name}")
            @storage.create(blob_info, input)

            input.close
            @logger.info("Uploaded file to #{@bucket}/#{blob_name}")
          rescue => e
            @logger.error("Failed to upload file", :exception => e)

            # TODO(rdc): limit retries?
            sleep 1
            retry
          end
        end

        def initialize_storage json_key_path
          if json_key_path.empty?
            return com.google.cloud.storage.StorageOptions.getDefaultInstance().getService()
          end

          key_file = java.io.FileInputStream.new json_key_path
          credentials = com.google.auth.oauth2.ServiceAccountCredentials.fromStream key_file

          return com.google.cloud.storage.StorageOptions.newBuilder()
              .setCredentials(credentials)
              .build()
              .getService()
        end
      end
    end
  end
end