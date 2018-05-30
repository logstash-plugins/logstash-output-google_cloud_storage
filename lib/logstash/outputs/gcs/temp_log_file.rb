# encoding: utf-8
require 'zlib'
require 'concurrent'
require 'time'

module LogStash
  module Outputs
    module Gcs
      # TempLogFile writes events to a file.
      class TempLogFile
        attr_reader :fd

        def initialize(path, gzip)
          @path = path
          @lock = Concurrent::ReentrantReadWriteLock.new

          @raw_fd = File.new(path, 'a+')
          @gz_fd = gzip ? Zlib::GzipWriter.new(@raw_fd) : nil

          @fd = @gz_fd || @raw_fd

          @last_sync = Time.now
        end

        def writeln(line)
          @lock.with_write_lock do
            @fd.write(line)
            @fd.write('\n')
          end
        end

        def fsync
          @lock.with_write_lock do
            @gz_fd.flush unless @gz_fd.nil?
            @raw_fd.fsync
            @last_sync = Time.now
          end
        end

        def close!
          @lock.with_write_lock do
            fsync

            @gz_fd.close unless @gz_fd.nil?
            @raw_fd.close
          end
        end

        def size
          @lock.with_read_lock do
            File.stat(@path).size
          end
        end

        def to_path
          @path
        end

        def time_since_sync
          @lock.with_read_lock do
            Time.now - @last_sync
          end
        end
      end
    end
  end
end
