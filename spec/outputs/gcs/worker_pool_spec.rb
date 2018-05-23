# encoding: utf-8
require 'logstash/outputs/gcs/worker_pool'

describe LogStash::Outputs::Gcs::WorkerPool do
  describe '#post' do
    it 'runs the task in the same thread if synchronous' do
      pool = LogStash::Outputs::Gcs::WorkerPool.new(5, true)
      expect(pool.workers).to_not receive(:post)

      pool.post { 1 + 2 }
    end

    it 'runs the task in a different thread if asynchronous' do
      pool = LogStash::Outputs::Gcs::WorkerPool.new(5, false)
      expect(pool.workers).to receive(:post)

      pool.post { 1 + 2 }
    end

    it 'raises an error if the pool is already stopped' do
      pool = LogStash::Outputs::Gcs::WorkerPool.new(5, true)
      pool.stop!

      expect{ pool.post{} }.to raise_error(RuntimeError)
    end
  end
end