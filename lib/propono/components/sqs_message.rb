module Propono
  class SqsMessage
    attr_reader :message, :receipt_handle, :failure_count
    def initialize(raw_message)
      raw_body = raw_message.body
      @raw_body_json = JSON.parse(raw_body)
      body = @raw_body_json['message']

      # @context        = Propono::Utils.symbolize_keys body
      # @failure_count  = context[:num_failures] || 0
      @message        = body
      @receipt_handle = raw_message.receipt_handle
    end

    def ==(other)
      other.is_a?(SqsMessage) && other.receipt_handle == @receipt_handle
    end
  end
end
