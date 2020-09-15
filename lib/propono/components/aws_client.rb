require 'aws-sdk-sqs'

module Propono
  class AwsClient
    attr_reader :aws_config
    def initialize(aws_config)
      @aws_config = aws_config
    end

    def send_to_sqs(queue, message, message_group_id)
      sqs_client.send_message(
        queue_url: queue.url,
        message_body: message.to_json,
        message_group_id: message_group_id,
        message_deduplication_id: SecureRandom.uuid
      )
    end

    def create_queue(name, additional_attributes)
      name += '.fifo' unless name.end_with? '.fifo'
      url = sqs_client.create_queue(queue_name: name, attributes: { 'FifoQueue' => 'true' }).queue_url
      attributes = sqs_client.get_queue_attributes(queue_url: url, attribute_names: ["QueueArn"]).attributes
      attributes.merge!(additional_attributes)
      Queue.new(url, attributes)
    end

    def set_sqs_policy(queue, policy)
      return
      sqs_client.set_queue_attributes(
        queue_url: queue.url,
        attributes: { Policy: policy }
      )
    end

    def read_from_sqs(queue, num_messages, long_poll: true, visibility_timeout: nil)
      wait_time_seconds = long_poll ? 20 : 0
      visibility_timeout ||= 30
      sqs_client.receive_message(
        queue_url: queue.url,
        wait_time_seconds: wait_time_seconds,
        max_number_of_messages: num_messages,
        visibility_timeout: visibility_timeout
      ).messages
    end

    def delete_from_sqs(queue, receipt_handle)
      sqs_client.delete_message(
        queue_url: queue.url,
        receipt_handle: receipt_handle
      )
    end

    private

    def sqs_client
      @sqs_client ||= Aws::SQS::Client.new(aws_config.aws_options)
    end
  end
end
