# encoding: utf-8
require 'logstash/outputs/gcs/log_rotate'
require 'logstash/outputs/gcs/path_factory'
require 'logstash/outputs/gcs/temp_log_file'

describe LogStash::Outputs::Gcs::LogRotate do
  let(:tempdir){ Stud::Temporary.directory }
  let(:path_factory) do
    LogStash::Outputs::Gcs::PathFactoryBuilder.build do |builder|
      builder.set_directory tempdir
      builder.set_prefix 'prefix'
      builder.set_include_host true
      builder.set_date_pattern ''
      builder.set_include_part true
      builder.set_include_uuid true
      builder.set_is_gzipped true
    end
  end
  let(:open_file_1) { double('open-temp-1', :size => 5, :path => 'one', :close! => true, :time_since_sync => 10, :fsync => true)}
  let(:open_file_2) { double('open-temp-2', :size => 5, :path => 'two', :close! => true, :time_since_sync => 60, :fsync => true)}

  describe '#initialize' do
    it 'opens the first file' do
      expect(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_1)

      LogStash::Outputs::Gcs::LogRotate.new(path_factory, 10, false, 30)
    end
  end

  describe '#write' do
    subject { LogStash::Outputs::Gcs::LogRotate.new(path_factory, 10, false, 30) }

    it 'does not rotate if size is small and path is the same' do
      expect(path_factory).to receive(:should_rotate?).and_return(false)
      # once for init
      expect(path_factory).to receive(:rotate_path!).once

      subject.write('foo')
    end

    it 'rotates the file if the size is too big' do
      # once for init, once for writeln
      expect(path_factory).to receive(:rotate_path!).twice

      subject.write('this line is longer than ten characters' * 1000)
      subject.write('flush')
    end

    it 'rotates the file if the path changed' do
      expect(path_factory).to receive(:should_rotate?).and_return(true)
      # once for init, once for writeln
      expect(path_factory).to receive(:rotate_path!).twice

      subject.write('foo')
    end

    it 'writes the message' do
      expect(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_1)
      expect(open_file_1).to receive(:write).with('foo', 'bar')

      subject.write('foo', 'bar')
    end

    it 'does not write if there are no parameters' do
      expect(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_1)
      expect(open_file_1).not_to receive(:write)

      subject.write
    end

    it 'does not fsync if delta less than limit' do
      expect(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_1)
      expect(open_file_1).not_to receive(:fsync)

      subject.write
    end

    it 'fsyncs if delta greater than limit' do
      expect(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_2)
      expect(open_file_2).to receive(:fsync)

      subject.write
    end
  end

  describe '#rotate_log!' do
    subject { LogStash::Outputs::Gcs::LogRotate.new(path_factory, 10, false, 30) }

    before :each do
      allow(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_1, open_file_2)
    end

    it 'closes the old file' do
      expect(open_file_1).to receive(:close!)

      subject.rotate_log!
    end

    it 'calls the callback with the old file name' do
      value = nil
      subject.on_rotate { |old_path| value = old_path }

      subject.rotate_log!
      expect(value).to eq(open_file_1.path)
    end

    it 'opens a new file based on the new path' do
      expect(LogStash::Outputs::Gcs::LogFileFactory).to receive(:create).and_return(open_file_1, open_file_2)
      expect(open_file_2).to receive(:write).with('foo', 'bar')

      subject.rotate_log!
      subject.write('foo', 'bar')
    end
  end

  describe '#on_rotate' do
    subject { LogStash::Outputs::Gcs::LogRotate.new(path_factory, 10, false, 30) }

    it 'replaces an existing callback' do
      value = :none

      subject.on_rotate { value = :first }
      subject.on_rotate { value = :second }

      subject.rotate_log!
      expect(value).to eq(:second)
    end
  end
end