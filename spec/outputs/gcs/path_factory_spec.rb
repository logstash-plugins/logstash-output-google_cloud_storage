# encoding: utf-8
require 'logstash/outputs/gcs/path_factory'

describe LogStash::Outputs::Gcs::PathFactory do
  describe '#initialize' do
    it 'includes optional fields if requested' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'path/to/directory'
        builder.set_prefix 'prefix'
        builder.set_include_host true
        builder.set_date_pattern ''
        builder.set_include_part true
        builder.set_include_uuid true
        builder.set_is_gzipped true
      end

      vars = {
          prefix: 'prefix',
          host: 'hostname',
          date: '2018-01-01',
          uuid: '00000000-0000-0000-0000-000000000000',
          partf: '333'
      }

      expected = 'prefix_hostname_2018-01-01.part333.00000000-0000-0000-0000-000000000000.log.gz'
      expected = File.join('path/to/directory', expected)

      actual = pf.current_path(vars)

      expect(actual).to eq(expected)
    end

    it 'excludes optional fields if not requested' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'path/to/directory'
        builder.set_prefix 'prefix'
        builder.set_include_host false
        builder.set_date_pattern ''
        builder.set_include_part false
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end

      vars = {
          prefix: 'prefix',
          host: 'hostname',
          date: '2018-01-01',
          uuid: '00000000-0000-0000-0000-000000000000',
          partf: '333'
      }

      expected = 'prefix_2018-01-01.log'
      expected = File.join('path/to/directory', expected)

      actual = pf.current_path(vars)

      expect(actual).to eq(expected)
    end

    it 'loads a path immediately' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory ''
        builder.set_prefix ''
        builder.set_include_host false
        builder.set_date_pattern ''
        builder.set_include_part false
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end

      expect(pf.current_path).to_not eq(nil)
    end

    it 'recovers the starting part number' do
      contents = ['pre_date.part009.log.gz', 'pre_date.part091.log.gz', 'pre_date.part000.log.gz']

      allow(::File).to receive(:directory?).with('dir').and_return(true)
      allow(Dir).to receive(:glob).and_return(contents)

      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'dir'
        builder.set_prefix 'pre'
        builder.set_include_host false
        builder.set_date_pattern 'date'
        builder.set_include_part true
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end

      expect(pf.current_path).to include('part092')
    end
  end

  describe 'rotate_path!' do
    it 'increments the part number if the base has not changed' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'dir'
        builder.set_prefix 'pre'
        builder.set_include_host false
        builder.set_date_pattern 'date'
        builder.set_include_part true
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end

      expect(pf.current_path).to eq(File.join('dir', 'pre_date.part000.log'))

      pf.rotate_path!
      expect(pf.current_path).to eq(File.join('dir', 'pre_date.part001.log'))
    end

    it 'resets the part number if the base has changed' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'dir'
        builder.set_prefix 'pre'
        builder.set_include_host false
        builder.set_date_pattern '%s'
        builder.set_include_part true
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end
      expect(pf.current_path).to include('part000')
      sleep(1)
      pf.rotate_path!
      expect(pf.current_path).to include('part000')
    end

    it 'returns the path being rotated out' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'dir'
        builder.set_prefix 'pre'
        builder.set_include_host false
        builder.set_date_pattern 'date'
        builder.set_include_part true
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end
      last = pf.current_path
      after = pf.rotate_path!
      expect(after).to eq(last)
    end
  end

  describe 'should_rotate?' do
    it 'returns false when the times in the bases are the same' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory ''
        builder.set_prefix ''
        builder.set_include_host false
        builder.set_date_pattern ''
        builder.set_include_part false
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end
      sleep 1.0
      expect(pf.should_rotate?).to eq(false)
    end

    it 'returns true when the times in the bases are different' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory ''
        builder.set_prefix ''
        builder.set_include_host false
        builder.set_date_pattern '%N'
        builder.set_include_part false
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end
      sleep 1.0
      expect(pf.should_rotate?).to eq(true)
    end
  end

  describe 'current_path' do
    it 'joins the directory and filename' do
      pf = LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
        builder.set_directory 'dir'
        builder.set_prefix 'pre'
        builder.set_include_host false
        builder.set_date_pattern 'date'
        builder.set_include_part false
        builder.set_include_uuid false
        builder.set_is_gzipped false
      end

      expect(pf.current_path).to eq(File.join('dir', 'pre_date.log'))
    end
  end
end
