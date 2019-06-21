module Propono
  class QueueSubscription

    attr_reader :aws_client, :propono_config, :topic_arn, :queue_name, :queue, :failed_queue, :corrupt_queue, :slow_queue

    def self.create(*args)
      new(*args).tap do |subscription|
        subscription.create
      end
    end

    def initialize(aws_client, propono_config, topic_name)
      @aws_client = aws_client
      @propono_config = propono_config
      @topic_name = topic_name
      @suffixed_topic_name = "#{topic_name}#{propono_config.queue_suffix}"
      @suffixed_slow_topic_name = "#{topic_name}#{propono_config.queue_suffix}-slow"
      @queue_name = "#{propono_config.application_name.tr(" ", "_")}-#{@suffixed_topic_name}"
    end

    def create
      raise ProponoError.new("topic_name is nil") unless @topic_name
      create_and_subscribe_main_queue
      create_and_subscribe_slow_queue
      create_misc_queues
    end

    def create_and_subscribe_main_queue
      @queue = aws_client.create_queue(queue_name: "#{queue_name}.fifo", attributes: { FifoQueue: true })
      topic = aws_client.create_topic(@suffixed_topic_name)
      aws_client.subscribe_sqs_to_sns(@queue, topic)
      aws_client.set_sqs_policy(@queue, generate_policy(@queue, topic))
    end

    def create_misc_queues
      @failed_queue = aws_client.create_queue(queue_name: "#{queue_name}-failed.fifo", attributes: { FifoQueue: true })
      @corrupt_queue = aws_client.create_queue(queue_name: "#{queue_name}-corrupt.fifo", attributes: { FifoQueue: true })
    end

    def create_and_subscribe_slow_queue
      @slow_queue = aws_client.create_queue(queue_name: "#{queue_name}-slow.fifo", attributes: { FifoQueue: true })
      slow_topic = aws_client.create_topic(@suffixed_slow_topic_name)
      aws_client.subscribe_sqs_to_sns(@slow_queue, slow_topic)
      aws_client.set_sqs_policy(@slow_queue, generate_policy(@slow_queue, slow_topic))
    end

    private

    def generate_policy(queue, topic)
      <<-EOS
        {
          "Version": "2008-10-17",
          "Id": "#{queue.arn}/SQSDefaultPolicy",
          "Statement": [
            {
              "Sid": "#{queue.arn}-Sid",
              "Effect": "Allow",
              "Principal": {
                "AWS": "*"
              },
              "Action": "SQS:*",
              "Resource": "#{queue.arn}",
              "Condition": {
                "StringEquals": {
                  "aws:SourceArn": "#{topic.arn}"
                }
              }
            }
          ]
        }
      EOS
    end
  end
end
