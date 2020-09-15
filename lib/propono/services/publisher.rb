require 'socket'

module Propono
  class PublisherError < ProponoError
  end

  class Publisher
    def self.publish(*args)
      new(*args).publish
    end

    attr_reader :aws_client, :propono_config, :topic_name, :message, :id, :async

    def initialize(aws_client, propono_config, topic_name, message, async: false, id: nil, message_group_id:)
      raise PublisherError.new("Topic is nil") if topic_name.nil?
      raise PublisherError.new("Message is nil") if message.nil?

      @aws_client = aws_client
      @propono_config = propono_config
      @topic_name = topic_name
      @message = message
      @async = async
      @message_group_id = message_group_id

      random_id = SecureRandom.hex(3)
      @id = id ? "#{id}-#{random_id}" : random_id
    end

    def publish
      propono_config.logger.info "Propono [#{id}]: Publishing #{message} to #{topic_name}"
      async ? publish_asyncronously : publish_syncronously
    end

    private

    def publish_asyncronously
      Thread.new { publish_syncronously }
    end

    def publish_syncronously
      begin
        queue = aws_client.create_queue(topic_name, { FifoQueue: true })
      rescue => e
        propono_config.logger.error "Propono [#{id}]: Failed to get or create topic #{topic_name}: #{e}"
        raise
      end

      begin
        aws_client.send_to_sqs(queue, body, @message_group_id)
      rescue => e
        propono_config.logger.error "Propono [#{id}]: Failed to send via sns: #{e}"
        raise
      end
    end

    def body
      {
        id: id,
        message: message
      }
    end
  end
end
