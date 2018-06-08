# encoding: utf-8
require_relative "../spec_helper"
require "google/api_client"
require "tempfile"

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
      encode_test({
                      :output_format => 'json',
                      :event => {'message' => 'contents'},
                      :expected => "{\"message\":\"contents\"}\n"
                  })
    end

    it 'should convert to a string if output_format is plain' do
      encode_test({
                      :output_format => 'plain',
                      :event => {'message' => 'contents'},
                      :expected => "1970-01-01T00:00:00.000Z source: contents\n"
                  })
    end

    it 'should use the plain format if nothing is specified' do
      encode_test({
                      :output_format => nil,
                      :event => {'message' => 'contents'},
                      :expected => "1970-01-01T00:00:00.000Z source: contents\n"
                  })
    end

    it 'should call the codec if output_format is "use-codec"' do
      encode_test({
                      :output_format => 'use-codec',
                      :event => {'message' => 'contents'},
                      :expected => "1970-01-01T00:00:00.000Z source: contents"
                  })
    end
  end
end


def encode_test(params)
  config = {
      "bucket" => "",
      "key_path" => "",
      "service_account" => "",
      "uploader_interval_secs" => 0.1,
      "upload_synchronous" => true
  }

  format = params[:output_format]
  unless format.nil?
    config["output_format"] = format
  end

  subject = LogStash::Outputs::GoogleCloudStorage.new(config)
  subject.register

  event = LogStash::Event.new(params[:event])
  event.timestamp = LogStash::Timestamp.at(0)
  #event.source = 'source'

  result = subject.encode(event)

  subject.close

  expect(result).to eq(params[:expected])
end
