# encoding: utf-8
require 'thread'

module LogStash
  module Outputs
    module Gcs
      class Schedule
        def self.every(period, &block)
          Scheduler.new(period, &block)
        end

        def initialize(period, &block)
          @task_thread = Thread.new do
            while true
              sleep period
              block.call
            end
          end
        end

        def kill!
          Thread.kill(@task_thread)
        end
      end
    end
  end
end