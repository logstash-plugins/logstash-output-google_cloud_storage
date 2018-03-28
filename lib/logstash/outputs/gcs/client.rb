# Author: Joseph Lewis III <jlewisiii@google.com>
# Date: 2018-02-23
#
# Copyright 2018 Google Inc.
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
#
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