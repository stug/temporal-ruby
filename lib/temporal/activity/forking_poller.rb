require 'temporal/activity/task_processor'
require 'temporal/connection'
require 'temporal/error_handler'
require 'temporal/metric_keys'
require 'temporal/middleware/chain'
require 'temporal/scheduled_thread_pool'

require 'grpc'

module Temporal
  class Activity
    class ForkingPoller
      DEFAULT_OPTIONS = {
        poll_retry_seconds: 0
      }.freeze

      def initialize(namespace, task_queue, activity_lookup, config, middleware = [], options = {})
        @namespace = namespace
        @task_queue = task_queue
        @activity_lookup = activity_lookup
        @config = config
        @middleware = middleware
        @shutting_down = false
        @options = DEFAULT_OPTIONS.merge(options)
      end

      def start  # TODO: maybe just get rid of this
        @shutting_down = false
        poll_loop
      end

      def stop_polling  # TODO: how to do shutdown
        @shutting_down = true
        Temporal.logger.info('Shutting down activity poller', { namespace: namespace, task_queue: task_queue })
      end

      def cancel_pending_requests
        connection.cancel_polling_request
      end

      private

      attr_reader :namespace, :task_queue, :activity_lookup, :config, :middleware, :options, :thread

      def connection
        @connection ||= Temporal::Connection.generate(config.for_connection)
      end

      def shutting_down?
        @shutting_down
      end

      def poll_loop
        # Prevent the poller thread from silently dying
        # Thread.current.abort_on_exception = true

        last_poll_time = Time.now
        metrics_tags = { namespace: namespace, task_queue: task_queue }.freeze

        loop do
          return if shutting_down?  # TODO: not sure what should happen to this

          time_diff_ms = ((Time.now - last_poll_time) * 1000).round
          Temporal.metrics.timing(Temporal::MetricKeys::ACTIVITY_POLLER_TIME_SINCE_LAST_POLL, time_diff_ms, metrics_tags)
          Temporal.logger.debug("Polling activity task queue", { namespace: namespace, task_queue: task_queue })

          task = poll_for_task
          last_poll_time = Time.now

          Temporal.metrics.increment(
            Temporal::MetricKeys::ACTIVITY_POLLER_POLL_COMPLETED,
            metrics_tags.merge(received_task: (!task.nil?).to_s)
          )

          next unless task&.activity_type

          # thread_pool.schedule { process(task) }
          process(task)
        end
      end

      def poll_for_task
        connection.poll_activity_task_queue(namespace: namespace, task_queue: task_queue)
      rescue ::GRPC::Cancelled
        # We're shutting down and we've already reported that in the logs
        nil
      rescue StandardError => error
        Temporal.logger.error("Unable to poll activity task queue", { namespace: namespace, task_queue: task_queue, error: error.inspect })

        Temporal::ErrorHandler.handle(error, config)

        sleep(poll_retry_seconds)

        nil
      end

      def process(task)
				Temporal.logger.info("forking to run activity")

				GRPC.prefork

				pid = fork do
					GRPC.postfork_child

					# TODO: needs to be closed
					heartbeat_thread_pool_for_fork = 
						ScheduledThreadPool.new(
							1,
							@config,
							{
								pool_name: 'heartbeat',
								namespace: namespace,
								task_queue: task_queue
							}
						)
					process_inner(task, heartbeat_thread_pool_for_fork)
				end

				GRPC.postfork_parent

				Temporal.logger.info("waiting on forked process (pid #{pid})")
				Process.wait(pid)
				Temporal.logger.info("forked process done!")
      end

      def process_inner(task, heartbeat_thread_pool)
        middleware_chain = Middleware::Chain.new(middleware)

        TaskProcessor.new(task, task_queue, namespace, activity_lookup, middleware_chain, config, heartbeat_thread_pool).process
      end

      def poll_retry_seconds
        @options[:poll_retry_seconds]
      end
    end
  end
end
