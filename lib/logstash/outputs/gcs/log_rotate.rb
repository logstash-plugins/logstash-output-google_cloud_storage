# encoding: utf-8
require 'thread'
require 'zlib'

module LogStash
  module Outputs
    module Gcs
      # PathFactory creates paths for rotating files.
      class LogRotate
        def initialize(path_factory, max_file_size_bytes, gzip, flush_interval_secs)
          @path_factory = path_factory
          @max_file_size_bytes = max_file_size_bytes
          @gzip = gzip
          @flush_interval_secs = flush_interval_secs

          rotate_log!
        end

        def write(message=nil)
          if should_rotate?
            old_path = initialize_next_log!
            yield old_path unless old_path.nil?
          end

          unless message.nil?
            @temp_file.write(message)
            @temp_file.write('\n')
          end

          sync_log_file
        end

        def rotate_log!
          old_path = initialize_next_log!
          yield old_path unless old_path.nil?
        end

        private

        def should_rotate?
          path_changed = @path_factory.should_rotate?
          too_big = @max_file_size_bytes > 0 && @temp_file.size >= @max_file_size_bytes

          path_changed || too_big
        end

        def initialize_next_log!
          if @temp_file.nil?
            old_path = nil
          else
            old_path = @temp_file.to_path
            @temp_file.fsync
            @temp_file.close
          end

          @path_factory.rotate_path!
          open_current_file

          old_path
        end

        def open_current_file
          path = @path_factory.current_path

          fd = File.new(path, 'a')
          fd = Zlib::GzipWriter.new(fd) if @gzip

          @temp_file = GCSIOWriter.new(fd)
        end

        def sync_log_file
          now = Time.now
          @last_flush_cycle = @last_flush_cycle || now

          if now - @last_flush_cycle >= @flush_interval_secs
            @temp_file.fsync
            @last_flush_cycle = now
          end
        end
      end

      ##
      # Wrapper class that abstracts which IO being used (for instance, regular
      # files or GzipWriter.
      #
      # Inspired by lib/logstash/outputs/file.rb.
      class GCSIOWriter
        attr_accessor :active

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
      end
    end
  end
end