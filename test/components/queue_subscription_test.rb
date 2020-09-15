require File.expand_path('../../test_helper', __FILE__)

module Propono
  class QueueSubscriptionTest < Minitest::Test
    def setup
      super
      @suffix = "-suf"
      propono_config.queue_suffix = @suffix
    end

    def teardown
      super
      propono_config.queue_suffix = ""
    end

    def test_create_calls_submethods
      subscription = QueueSubscription.new(aws_client, propono_config, "foobar")
      subscription.expects(:create_main_queue)
      subscription.create
    end

    def test_create_main_queue
      policy = "Some policy"
      topic_name = "SomeName"

      subscription = QueueSubscription.new(aws_client, propono_config, topic_name)
      subscription.stubs(generate_policy: policy)
      queue_name = subscription.send(:queue_name)

      topic = mock
      queue = mock
      aws_client.expects(:create_queue).with(queue_name, {:FifoQueue => true}).returns(queue)
      aws_client.expects(:set_sqs_policy).with(queue, policy)

      subscription.create
      assert_equal queue, subscription.queue
    end

    def test_subscription_queue_name
      propono_config.application_name = "MyApp"

      topic_name = "Foobar"
      subscription = QueueSubscription.new(aws_client, propono_config, topic_name)

      assert_equal "Foobar", subscription.send(:queue_name)
    end

    def test_create_raises_with_nil_topic
      subscription = QueueSubscription.new(aws_client, propono_config, nil)
      assert_raises ProponoError do
        subscription.create
      end
    end
  end
end
