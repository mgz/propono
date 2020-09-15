module Propono
  class QueueSubscription

    attr_reader :aws_client, :propono_config, :topic_arn, :queue_name, :queue

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
      @queue_name = topic_name
    end

    def create
      raise ProponoError.new("topic_name is nil") unless @topic_name
      create_main_queue
    end

    def create_main_queue
      @queue = aws_client.create_queue(queue_name, { FifoQueue: true })
      aws_client.set_sqs_policy(@queue, generate_policy(@queue))
    end

    private

    def generate_policy(queue)
      return
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
            }
          ]
        }
      EOS
    end
  end
end
