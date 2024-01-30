require 'temporal/errors'
require 'temporal/activity/forking_poller'
require 'temporal/execution_options'
require 'temporal/executable_lookup'
require 'temporal/middleware/entry'

module Temporal
  class ForkingActivityWorker
    def initialize(
      config = Temporal.configuration,
      activity_poll_retry_seconds: Temporal::Activity::ForkingPoller::DEFAULT_OPTIONS[:poll_retry_seconds]
    )
      @config = config
      @activities = Hash.new { |hash, key| hash[key] = ExecutableLookup.new }
      @pollers = []
      @activity_middleware = []
      @shutting_down = false
      @activity_poller_options = {
        poll_retry_seconds: activity_poll_retry_seconds
      }
    end

    def register_activity(activity_class, options = {})
      namespace_and_task_queue, execution_options = executable_registration(activity_class, options)
      @activities[namespace_and_task_queue].add(execution_options.name, activity_class)
    end

    # Register one special activity that you want to intercept any unknown activities,
    # perhaps so you can delegate work to other classes, somewhat analogous to ruby's method_missing.
    # Only one dynamic Activity may be registered per task queue.
    # Within Activity#execute, you may retrieve the name of the unknown class via activity.name.
    def register_dynamic_activity(activity_class, options = {})
      namespace_and_task_queue, execution_options = executable_registration(activity_class, options)
      begin
        @activities[namespace_and_task_queue].add_dynamic(execution_options.name, activity_class)
      rescue Temporal::ExecutableLookup::SecondDynamicExecutableError => e
        raise Temporal::SecondDynamicActivityError,
              "Temporal::Worker#register_dynamic_activity: cannot register #{execution_options.name} "\
              "dynamically; #{e.previous_executable_name} was already registered dynamically for task queue "\
              "'#{execution_options.task_queue}', and there can be only one."
      end
    end

    def add_activity_middleware(middleware_class, *args)
      @activity_middleware << Middleware::Entry.new(middleware_class, args)
    end

    def start
      # TODO: validate only one poller (probably at registration time)
      # TODO: shutdown/trap signals?
      activities.each_pair do |(namespace, task_queue), lookup|
        pollers << activity_poller_for(namespace, task_queue, lookup)
      end
      pollers.first.start
      stop
    end

    def stop  # TODO
    end

    private

    attr_reader :config, :activity_poller_options, :workflow_poller_options,
                :activities, :workflows, :pollers,
                :workflow_task_middleware, :workflow_middleware, :activity_middleware

    def shutting_down?
      @shutting_down
    end

    def on_started_hook; end
    def while_stopping_hook; end
    def on_stopped_hook; end

    def activity_poller_for(namespace, task_queue, lookup)
      Activity::ForkingPoller.new(namespace, task_queue, lookup.freeze, config, activity_middleware, activity_poller_options)
    end

    def executable_registration(executable_class, options)
      execution_options = ExecutionOptions.new(executable_class, options, config.default_execution_options)
      key = [execution_options.namespace, execution_options.task_queue]
      [key, execution_options]
    end

    def trap_signals
    end
  end
end
