# encoding: utf-8
require_relative "../spec_helper"
require "tempfile"

describe LogStash::Outputs::GoogleCloudStorage do

  let(:javaclient) { double("google-java-client") }
  let(:javastorage) { double("google-java-client-storage") }

  subject { described_class.new(config) }
  let(:config) { {"bucket" => "", "uploader_interval_secs" => 0.1 } }

  before(:each) do
    allow(LogStash::Outputs::Gcs::Client).to receive(:new).and_return(:javaclient)
    allow(javaclient).to receive(:initialize_storage).and_return(:javastorage)
  end

  it "should register without errors" do
    expect { subject.register }.to_not raise_error
  end

  describe "file size based decider for uploading" do
    let(:upload_queue) { Queue.new }
    let(:content) { }
    before(:each) do
      allow(subject).to receive(:new_upload_queue).and_return(upload_queue)
      subject.send(:initialize_upload_queue)
      subject.send(:initialize_temp_directory)
      subject.send(:initialize_path_factory)
      subject.send(:open_current_file)
      current_file = upload_queue.pop
      File.write(current_file, content) if content
      upload_queue.push(current_file)
      subject.send(:initialize_next_log)
    end

    context "when spooled file is empty" do
      let(:content) { nil }
      it "doesn't get uploaded" do
        expect(subject).to_not receive(:upload_object)
        subject.send(:upload_from_queue)
      end
    end

    context "when spooled file has content" do
      let(:content) { "hello" }
      it "gets uploaded" do
        expect(subject).to receive(:upload_object)
        subject.send(:upload_from_queue)
      end
    end
  end
end
