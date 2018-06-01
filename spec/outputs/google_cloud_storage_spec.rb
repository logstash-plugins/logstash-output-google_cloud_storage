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

  it "should register without errors" do
    expect { subject.register }.to_not raise_error
  end
end
