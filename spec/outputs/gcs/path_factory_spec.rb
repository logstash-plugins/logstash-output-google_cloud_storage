# encoding: utf-8
require 'logstash/outputs/gcs/path_factory'

describe LogStash::Outputs::Gcs::PathFactory do
  describe '#initialize' do
    it 'includes optional fields if requested' do
      pf = LogStash::Outputs::Gcs::PathFactory.new(
          'path/to/directory',
          'prefix',
          true,
          '',
          true,
          true,
          true
      )

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
      pf = LogStash::Outputs::Gcs::PathFactory.new(
          'path/to/directory',
          'prefix',
          false,
          '',
          false,
          false,
          false
      )

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
      pf = LogStash::Outputs::Gcs::PathFactory.new('', '', false, '', false, false, false)

      expect(pf.current_path).to_not eq(nil)
    end

    it 'recovers the starting part number' do
      contents = ['pre_date.part009.log.gz', 'pre_date.part091.log.gz', 'pre_date.part000.log.gz']

      allow(::File).to receive(:directory?).with('dir').and_return(true)
      allow(Dir).to receive(:glob).and_return(contents)

      pf = LogStash::Outputs::Gcs::PathFactory.new('dir', 'pre', false, 'date', true, false, false)

      expect(pf.current_path).to include('part092')
    end
  end

  describe 'rotate_path!' do
    it 'increments the part number if the base has not changed' do
      pf = LogStash::Outputs::Gcs::PathFactory.new('dir', 'pre', false, 'date', true, false, false)
      expect(pf.current_path).to eq(File.join('dir', 'pre_date.part000.log'))

      pf.rotate_path!
      expect(pf.current_path).to eq(File.join('dir', 'pre_date.part001.log'))
    end

    it 'resets the part number if the base has changed' do
      pf = LogStash::Outputs::Gcs::PathFactory.new('dir', 'pre', false, '%N', true, false, false)
      expect(pf.current_path).to include('part000')

      pf.rotate_path!
      expect(pf.current_path).to include('part000')
    end

    it 'returns the current_path' do
      pf = LogStash::Outputs::Gcs::PathFactory.new('dir', 'pre', false, 'date', true, false, false)
      after = pf.rotate_path!
      expect(after).to eq(File.join('dir', 'pre_date.part001.log'))
    end
  end

  describe 'should_rotate?' do
    it 'returns false when the times in the bases are the same' do
      pf = LogStash::Outputs::Gcs::PathFactory.new('', '', false, '', false, false, false)
      sleep 0.1
      expect(pf.should_rotate?).to eq(false)
    end

    it 'returns true when the times in the bases are different' do
      pf = LogStash::Outputs::Gcs::PathFactory.new('', '', false, '%N', false, false, false)
      sleep 0.1
      expect(pf.should_rotate?).to eq(true)
    end
  end

  describe 'current_path' do
    it 'joins the directory and filename' do
      pf = LogStash::Outputs::Gcs::PathFactory.new('dir', 'pre', false, 'date', false, false, false)
      expect(pf.current_path).to eq(File.join('dir', 'pre_date.log'))
    end
  end
end
