# encoding: utf-8
require 'logstash/outputs/gcs/temp_log_file'
require 'stud/temporary'
require 'zlib'

shared_examples 'a log file' do
  describe '#initialize' do
    it 'opens a file' do
      expect{subject.fd}.to_not raise_error
      expect(subject.fd).to_not be_nil
    end

    it 'sets the path' do
      expect{subject.path}.to_not raise_error
      expect(subject.path).to_not be_nil
    end

    it 'sets last sync' do
      expect{subject.time_since_sync}.to_not raise_error
    end
  end

  describe '#write' do
    it 'writes the content' do
      expect(subject.fd).to receive(:write).with('foo')
      expect(subject.fd).to receive(:write).with("\n")

      subject.write('foo', "\n")
    end

    it 'fails if the file is closed' do
      subject.close!

      expect{ subject.write('foo') }.to raise_error(IOError)
    end
  end

  describe '#fsync' do
    it 'fails if the file is closed' do
      subject.close!

      expect{ subject.fsync }.to raise_error(IOError)
    end
  end

  describe '#close!' do
    it 'fails if the file is closed' do
      subject.close!

      expect{ subject.close! }.to raise_error(IOError)
    end
  end

  describe '#size' do
    it 'gets the size of the file on disk' do
      subject.write('hello, world!')
      subject.fsync

      expect(subject.size).to eq(File.stat(subject.path).size)
    end

    it 'does not fail if the file is closed' do
      subject.close!

      expect{ subject.size }.to_not raise_error
    end
  end

  describe '#time_since_sync' do
    it 'returns a delta' do
      expect(Time).to receive(:now).and_return(Time.at(30), Time.at(40), Time.at(50))

      subject.fsync

      expect(subject.time_since_sync).to eq(10)
    end
  end
end

describe LogStash::Outputs::Gcs::PlainLogFile do
  let(:tempdir) { Stud::Temporary.directory }
  let(:path) { ::File.join(tempdir, 'logfile.log') }
  subject { LogStash::Outputs::Gcs::LogFileFactory.create(path, false, false) }

  it_behaves_like 'a log file'

  it 'creates a valid plain text file' do
    subject.write('Hello, world!')
    subject.close!
    data = File.read(path)

    expect(data).to eq('Hello, world!')
  end
end

describe LogStash::Outputs::Gcs::GzipLogFile do
  let(:tempdir) { Stud::Temporary.directory }
  let(:path) { ::File.join(tempdir, 'logfile.log') }
  subject { LogStash::Outputs::Gcs::LogFileFactory.create(path, true, false) }

  it_behaves_like 'a log file'

  it 'creates a valid gzip' do
    subject.write('Hello, world!')
    subject.close!

    Zlib::GzipReader.open(path) do |gz|
      expect(gz.read).to eq('Hello, world!')
    end
  end
end

describe LogStash::Outputs::Gcs::SynchronizedLogFile do
  let(:tempdir) { Stud::Temporary.directory }
  let(:path) { ::File.join(tempdir, 'logfile.log') }
  subject { LogStash::Outputs::Gcs::LogFileFactory.create(path, false, true) }

  it_behaves_like 'a log file'
end

describe 'gzip encoded file' do
  let(:tempdir) { Stud::Temporary.directory }
  let(:path) { ::File.join(tempdir, 'logfile.log') }
  subject { LogStash::Outputs::Gcs::LogFileFactory.create(path, false, false, true) }

  it_behaves_like 'a log file'

  it 'creates a valid gzip' do
    subject.write('Hello, world!')
    subject.close!

    Zlib::GzipReader.open(path) do |gz|
      expect(gz.read).to eq('Hello, world!')
    end
  end
end

describe 'double gzip encoded file' do
  let(:tempdir) { Stud::Temporary.directory }
  let(:path) { ::File.join(tempdir, 'logfile.log') }
  subject { LogStash::Outputs::Gcs::LogFileFactory.create(path, true, false, true) }

  it_behaves_like 'a log file'

  it 'creates a valid double gzip' do
    subject.write('Hello, world!')
    subject.close!

    Zlib::GzipReader.open(path) do |outer|
      Zlib::GzipReader.new(outer) do |inner|
        expect(inner.read).to eq('Hello, world!')
      end
    end
  end
end