# encoding: utf-8
require_relative "../spec_helper"
require "tempfile"

describe LogStash::Outputs::GoogleCloudStorage do

  let(:javaclient) { double("google-java-client") }
  let(:javastorage) { double("google-java-client-storage") }

  subject { described_class.new(config) }
  let(:config) { {"bucket" => "", "uploader_interval_secs" => 0.1, "upload_synchronous" => true} }

  before(:each) do
    allow(LogStash::Outputs::Gcs::Client).to receive(:new).and_return(:javaclient)
    allow(javaclient).to receive(:initialize_storage).and_return(:javastorage)
  end

  it "should register without errors" do
    expect { subject.register }.to_not raise_error
  end
end
