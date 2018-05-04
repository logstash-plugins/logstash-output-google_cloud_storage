# encoding: utf-8
require 'thread'

module LogStash
  module Outputs
    module Gcs
      # PathFactory creates paths for rotating files.
      class PathFactory
        def initialize(directory, prefix, include_host, date_pattern, include_part, include_uuid, is_gzipped)
          @path_lock = Mutex.new

          pattern = '%{prefix}'
          pattern += '_%{host}' if include_host
          pattern += '_%{date}'
          @base_pattern = pattern

          pattern += '.part%{partf}' if include_part
          pattern += '.%{uuid}' if include_uuid
          pattern += '.log'
          pattern += '.gz' if is_gzipped
          @pattern = pattern

          @prefix = prefix
          @directory = directory
          @date_pattern = date_pattern

          @part_number = starting_part
          @current = template_variables
        end

        # Rotates the path to the next one in sequence. If the path has a part number
        # and the base path (date/hostname) haven't changed the part number is incremented.
        def rotate_path!
          @path_lock.synchronize {
            @part_number = (next_base == current_base) ? @part_number + 1 : 0
            @current = template_variables
          }

          current_path
        end

        # Checks if the file is ready to rotate because the timestamp changed.
        def should_rotate?
          @path_lock.synchronize {
            next_base != current_base
          }
        end

        # Returns the full path to the current file including parent directory.
        def current_path(vars=nil)
          @path_lock.synchronize {
            filename = @pattern % (vars || @current)
            ::File.join(@directory, filename)
          }
        end

        private

        # search through the directory for a file with the same base, and if it exists,
        # set our part to be the max + 1 so we don't clobber existing files.
        def starting_part
          return 0 unless ::File.directory? @directory

          base_path = ::File.join(@directory, next_base)

          part_numbers = Dir.glob(base_path + '.part*').map do |item|
            match = /^.*\.part(?<part_num>\d+).*$/.match(item)
            next if match.nil?
            match[:part_num].to_i
          end

          part_numbers.any? ? part_numbers.max + 1 : 0
        end

        def template_variables
          {
              prefix: @prefix,
              host: Socket.gethostname,
              date: Time.now.strftime(@date_pattern),
              partf: '%03d' % @part_number,
              uuid: SecureRandom.uuid
          }
        end

        def next_base
          @base_pattern % template_variables
        end

        def current_base
          @base_pattern % @current
        end
      end

      # PathFactoryBuilder makes the long PathFactory constructor chain more readable.
      class PathFactoryBuilder
        def self.build
          builder = new
          yield builder
          builder.build_path_factory
        end

        def self.builder_setter(*names)
          names.each do |name|
            define_method("set_#{name}") {|arg| instance_variable_set("@#{name}", arg)}
          end
        end

        builder_setter :directory, :prefix, :include_host, :date_pattern, :include_part, :include_uuid, :is_gzipped

        def build_path_factory
          PathFactory.new(@directory, @prefix, @include_host, @date_pattern, @include_part, @include_uuid, @is_gzipped)
        end
      end
    end
  end
end