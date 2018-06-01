# encoding: utf-8
require 'zlib'
require 'concurrent'
require 'time'

module LogStash
  module Outputs
    module Gcs
      class LogFileFactory
        def self.create(path, gzip, synchronize=true)
          lf = LogStash::Outputs::Gcs::PlainLogFile.new(path)
          lf = LogStash::Outputs::Gcs::GzipLogFile.new(lf) if gzip
          lf = LogStash::Outputs::Gcs::SynchronizedLogFile.new(lf) if synchronize

          lf
        end
      end

      # TempLogFile writes events to a file.
      class PlainLogFile
        attr_reader :path, :fd

        def initialize(path)
          @path = path
          @fd = ::File.new(path, 'a+')
          @last_sync = Time.now
        end

        def write(*contents)
          contents.each { |c| @fd.write(c) }
        end

        def fsync
          @fd.fsync
          @last_sync = Time.now
        end

        def close!
          @fd.fsync
          @fd.close
        end

        def size
          ::File.stat(@path).size
        end

        def time_since_sync
          Time.now - @last_sync
        end
      end

      class GzipLogFile
        attr_reader :fd

        def initialize(child)
          @child = child
          @fd = Zlib::GzipWriter.new(child.fd)
        end

        def write(*contents)
          contents.each { |c| @fd.write(c) }
        end

        def fsync
          @fd.flush
          @child.fsync
        end

        def close!
          fsync
          # The Gzip writer closes the underlying IO after
          # appending the Gzip footer.
          @fd.close
        end

        def method_missing(method_name, *args, &block)
          @child.send(method_name, *args, &block)
        end
      end

      class SynchronizedLogFile
        def initialize(child)
          @child = child
          @lock = Concurrent::ReentrantReadWriteLock.new
        end

        def time_since_sync
          @lock.with_read_lock { @child.time_since_sync }
        end

        def path
          @lock.with_read_lock { @child.path }
        end

        def method_missing(method_name, *args, &block)
          # unless otherwise specified, get a write lock
          @lock.with_write_lock do
            @child.send(method_name, *args, &block)
          end
        end
      end
    end
  end
end
