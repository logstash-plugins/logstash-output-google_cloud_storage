# encoding: utf-8
require 'thread'
require 'concurrent'

module LogStash
  module Outputs
    module Gcs
      # WorkerPool creates a pool of workers that can handle jobs.
      class WorkerPool
        attr_reader :workers

        def initialize(max_threads, synchronous=false)
          @synchronous = synchronous

          # set queue depth to the be the same as the number of threads so
          # there's at most one pending job each when the plugin quits
          @workers = Concurrent::ThreadPoolExecutor.new(
            min_threads: 1,
            max_threads: max_threads,
            max_queue: max_threads,
            fallback_policy: :caller_runs
          )
        end

        # Submits a job to the worker pool, raises an error if the pool has
        # already been stopped.
        def post(&block)
          raise 'Pool already stopped' unless @workers.running?

          if @synchronous
            block.call
          else
            @workers.post do
              block.call
            end
          end
        end

        # Stops the worker pool
        def stop!
          @workers.shutdown
          @workers.wait_for_termination
        end
      end
    end
  end
end