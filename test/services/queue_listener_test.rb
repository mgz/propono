require File.expand_path('../../test_helper', __FILE__)

module Propono
  class QueueListenerTest < Minitest::Test

    def setup
      super
      @topic_name = "some-topic"

      @receipt_handle1 = "test-receipt-handle1"
      @receipt_handle2 = "test-receipt-handle2"
      @message1 = {cat: "Foobar 123"}
      @message2 = "Barfoo 543"
      @message1_id = "abc123"
      @message2_id = "987whf"
      @body1 = {id: @message1_id, message: @message1}
      @body2 = {id: @message2_id, message: @message2}

      @sqs_message1 = mock
      @sqs_message1.stubs(receipt_handle: @receipt_handle1, body: {"Message" => @body1.to_json}.to_json)
      @sqs_message2 = mock
      @sqs_message2.stubs(receipt_handle: @receipt_handle2, body: {"Message" => @body2.to_json}.to_json)

      @queue = mock.tap {|q| q.stubs(url: "foobar", arn: "qarn") }
      @topic = mock.tap {|t| t.stubs(arn: "tarn") }
      aws_client.stubs(
        create_queue: @queue
      )
      aws_client.stubs(:set_sqs_policy)

      @messages = [@sqs_message1, @sqs_message2]
      aws_client.stubs(read_from_sqs: @messages)
      aws_client.stubs(:delete_from_sqs)

      @listener = QueueListener.new(aws_client, propono_config, @topic_name) {}

      propono_config.num_messages_per_poll = 14
      propono_config.max_retries = 0
    end

    def test_listen_should_loop
      @listener.expects(:loop)
      @listener.listen
    end

    def test_listen_raises_with_nil_topic
      listener = QueueListener.new(aws_client, propono_config, nil) {}
      assert_raises ProponoError do
        listener.listen
      end
    end

    def test_drain_should_continue_if_queue_empty
      @listener.expects(:read_messages_from_queue).with(@queue, 10, long_poll: false).returns(false)
      @listener.drain
      assert true
    end

    def test_drain_raises_with_nil_topic
      listener = QueueListener.new(aws_client, propono_config, nil) {}
      assert_raises ProponoError do
        listener.drain
      end
    end

    def test_read_messages_should_subscribe
      queue = mock
      queue.stubs(:url)
      QueueSubscription.expects(:create).with(aws_client, propono_config, @topic_name).returns(mock(queue: queue))
      @listener.send(:read_messages)
    end

    def test_read_message_from_sqs
      max_number_of_messages = 5
      aws_client.expects(:read_from_sqs).with(@queue, max_number_of_messages, long_poll: true, visibility_timeout: nil)
      @listener.send(:read_messages_from_queue, @queue, max_number_of_messages)
    end

    def test_log_message_from_sqs
      propono_config.logger.expects(:info).with() {|x| x == "Propono: Received from sqs."}
      @listener.send(:read_messages_from_queue, @queue, propono_config.num_messages_per_poll)
    end

    def test_read_messages_calls_process_message_for_each_msg
      @listener.expects(:process_raw_message).with(@sqs_message1, @queue)
      @listener.expects(:process_raw_message).with(@sqs_message2, @queue)
      @listener.send(:read_messages_from_queue, @queue, propono_config.num_messages_per_poll)
    end

    def test_read_messages_does_not_call_process_messages_if_there_are_none
      aws_client.stubs(read_from_sqs: [])
      @listener.expects(:process_message).never
      @listener.send(:read_messages_from_queue, @queue, propono_config.num_messages_per_poll)
    end

    def test_exception_from_sqs_is_logged
      aws_client.stubs(:read_from_sqs).raises(StandardError)
      propono_config.logger.expects(:error).with("Unexpected error reading from queue #{@queue.url}")
      @listener.send(:read_messages_from_queue, @queue, propono_config.num_messages_per_poll)
    end

    def test_exception_from_sqs_returns_false
      aws_client.stubs(:read_from_sqs).raises(StandardError)
      refute @listener.send(:read_messages)
    end

    def test_ok_if_message_processor_is_nil
      messages_yielded = []
      @listener = QueueListener.new(aws_client, propono_config, @topic_name)

      @listener.send(:process_message, "")
      assert_equal messages_yielded.size, 0
    end

    def test_messages_are_deleted_if_there_is_an_exception_processing
      aws_client.expects(:delete_from_sqs).with(@queue, @receipt_handle1)
      aws_client.expects(:delete_from_sqs).with(@queue, @receipt_handle2)

      exception = StandardError.new("Test Error")
      @listener = QueueListener.new(aws_client, propono_config, @topic_name) { raise exception }
      @listener.stubs(:requeue_message_on_failure).with(SqsMessage.new(@sqs_message1), exception)
      @listener.stubs(:requeue_message_on_failure).with(SqsMessage.new(@sqs_message2), exception)
      @listener.send(:read_messages_from_queue, @queue, propono_config.num_messages_per_poll)
    end
  end
end
