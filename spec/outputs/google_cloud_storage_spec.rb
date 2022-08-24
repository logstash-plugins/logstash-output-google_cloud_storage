# encoding: utf-8
require_relative "../spec_helper"
require "tempfile"
require "json"

describe LogStash::Outputs::GoogleCloudStorage do

  let(:javaclient) { double("google-java-client") }
  let(:javastorage) { double("google-java-client-storage") }

  subject { described_class.new(config) }
  let(:config) { {"bucket" => "", "uploader_interval_secs" => 0.1, "upload_synchronous" => true} }

  before(:each) do
    allow(LogStash::Outputs::Gcs::Client).to receive(:new).and_return(:javaclient)
    allow(javaclient).to receive(:initialize_storage).and_return(:javastorage)
  end

  it 'should register without errors' do
    expect { subject.register }.to_not raise_error

    subject.close
  end

  describe '#encode' do
    it 'should dump the event hash if output_format is json' do
      encoded = encode_test({
                                :output_format => 'json',
                                :event => {'message' => 'contents'}
                            })

      expect(encoded.end_with?("\n")).to eq(true)

      encoded_hash = JSON.parse(encoded)
      expect(encoded_hash).to eq({
                                     'message' => 'contents',
                                     '@timestamp' => '1970-01-01T00:00:00Z',
                                     'host' => 'localhost',
                                     '@version' => '1'})
    end

    it 'should convert to a string if output_format is plain' do
      encoded = encode_test({
                      :output_format => 'plain',
                      :event => {'message' => 'contents'}
                  })

      expect(encoded).to match(/1970-01-01T00:00:00(\.000)?Z localhost contents\n/)

    end

    it 'should call the codec if output_format is blank' do
      encoded = encode_test({
                      :output_format => nil,
                      :event => {'message' => 'contents'}
                  })

      expect(encoded).to match(/1970-01-01T00:00:00(\.000)?Z localhost contents\n/)
    end

    it 'should call the codec if no output_format' do
      encoded = encode_test({
                                :event => {'message' => 'contents'}
                            })

      expect(encoded).to match(/1970-01-01T00:00:00(\.000)?Z localhost contents\n/)
    end
  end
end

def encode_test(params)
  config = {
      'bucket' => '',
      'service_account' => '',
      'uploader_interval_secs' => 10000,
      'upload_synchronous' => true,
  }
  config['output_format'] = params[:output_format] if params[:output_format]

  rotater = double('rotater')
  allow(rotater).to receive(:on_rotate)
  allow(rotater).to receive(:rotate_log!)

  allow(LogStash::Outputs::Gcs::LogRotate).to receive(:new).and_return(rotater)

  gcsout = LogStash::Outputs::GoogleCloudStorage.new(config)
  gcsout.disable_uploader = true
  gcsout.register

  event = LogStash::Event.new(params[:event])
  event.timestamp = LogStash::Timestamp.at(0)
  event.set('host', 'localhost')

  value = ''
  allow(rotater).to receive(:write){ |line| value = line }

  gcsout.multi_receive([event])
  gcsout.close

  value
end
