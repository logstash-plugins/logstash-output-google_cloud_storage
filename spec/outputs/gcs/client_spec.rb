# encoding: utf-8

require 'logstash/outputs/gcs/client'

describe LogStash::Outputs::Gcs::Client do

  # This test is mostly to make sure the Java types, signatures and classes
  # haven't changed being that JRuby is very relaxed.
  describe '#initialize' do
    let(:logger) { spy('logger') }

    it 'does not throw an error when initializing' do
      key_file = ::File.join('spec', 'fixtures', 'credentials.json')
      key_file = ::File.absolute_path(key_file)
      LogStash::Outputs::Gcs::Client.new('my-bucket', key_file, logger)
    end
  end
end