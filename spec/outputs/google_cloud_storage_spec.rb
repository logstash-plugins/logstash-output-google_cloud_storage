# encoding: utf-8
require_relative "../spec_helper"
require "google/api_client"
require "tempfile"
require "json"

describe LogStash::Outputs::GoogleCloudStorage do

  let(:client) { double("google-client") }
  let(:service_account) { double("service-account") }
  let(:key)    { "key" }

  subject { described_class.new(config) }
  let(:config) { {"bucket" => "", "key_path" => "", "service_account" => "", "uploader_interval_secs" => 0.1, "upload_synchronous" => true} }

  before(:each) do
    allow(Google::APIClient).to receive(:new).and_return(client)
    allow(client).to receive(:discovered_api).with("storage", "v1")
    allow(Google::APIClient::PKCS12).to receive(:load_key).with("", "notasecret").and_return(key)
    allow(Google::APIClient::JWTAsserter).to receive(:new).and_return(service_account)
    allow(client).to receive(:authorization=)
    allow(service_account).to receive(:authorize)
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
      expect(encoded_hash).to eq({'message' => 'contents', '@timestamp' => '1970-01-01T00:00:00.000Z', 'host' => 'localhost', '@version' => '1'})
    end

    it 'should convert to a string if output_format is plain' do
      encoded = encode_test({
                      :output_format => 'plain',
                      :event => {'message' => 'contents'}
                  })

      expect(encoded).to eq("1970-01-01T00:00:00.000Z localhost contents\n")

    end

    it 'should call the codec if output_format is "use-codec"' do
      encoded = encode_test({
                      :output_format => 'use-codec',
                      :event => {'message' => 'contents'}
                  })

      expect(encoded).to eq("1970-01-01T00:00:00.000Z localhost contents\n")
    end
  end
end

def encode_test(params)
  config = {
      "bucket" => "",
      "key_path" => "",
      "service_account" => "",
      "uploader_interval_secs" => 10000,
      "upload_synchronous" => true,
      "output_format" => params[:output_format]
  }

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
  allow(rotater).to receive(:write) do |line|
    value = line
  end

  gcsout.multi_receive([event])
  gcsout.close

  value
end
