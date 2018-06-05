# encoding: utf-8
require 'logstash/outputs/gcs/temp_log_file'
require 'concurrent'

module LogStash
  module Outputs
    module Gcs
      class LogRotate
        def initialize(path_factory, max_file_size_bytes, gzip, flush_interval_secs)
          @path_factory = path_factory
          @max_file_size_bytes = max_file_size_bytes
          @gzip = gzip
          @flush_interval_secs = flush_interval_secs

          @lock = Concurrent::ReentrantReadWriteLock.new
          @rotate_callback = nil

          rotate_log!
        end

        # writeln writes a message and carriage-return character to the open
        # log file, rotating and syncing it if necessary.
        #
        # nil messages do not get written, but may cause the log to rotate
        def writeln(message=nil)
          @lock.with_write_lock do
            rotate_log! if should_rotate?

            @temp_file.write(message, "\n") unless message.nil?

            @temp_file.fsync if @temp_file.time_since_sync >= @flush_interval_secs
          end
        end

        # rotate_log! closes the current log (if it exists), notifies the
        # handler, rolls the path over and opens a new log.
        #
        # Invariant: the old log will ALWAYS be closed and a new one will
        # ALWAYS be open at the completion of this function.
        def rotate_log!
          @lock.with_write_lock do
            unless @temp_file.nil?
              @temp_file.close!
              @rotate_callback.call(@temp_file.path) unless @rotate_callback.nil?
            end

            @path_factory.rotate_path!

            path = @path_factory.current_path
            @temp_file = LogStash::Outputs::Gcs::LogFileFactory.create(path, @gzip)
          end
        end

        # on_rotate sets a handler to be called when the log gets rotated.
        # The handler receives the path to the rotated out log as a string.
        def on_rotate(&block)
          @lock.with_write_lock do
            @rotate_callback = block
          end
        end

        private

        def should_rotate?
          @lock.with_read_lock do
            path_changed = @path_factory.should_rotate?
            rotate_on_size = @max_file_size_bytes > 0
            too_big = @temp_file.size >= @max_file_size_bytes

            path_changed || (rotate_on_size && too_big)
          end
        end
      end
    end
  end
end