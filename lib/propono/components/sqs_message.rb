module Propono
  class SqsMessage
    attr_reader :message, :receipt_handle, :failure_count
    def initialize(raw_message)
      raw_body = raw_message.body
      @message = JSON.parse(raw_body, symbolize_names: true)

      @receipt_handle = raw_message.receipt_handle
    end

    def ==(other)
      other.is_a?(SqsMessage) && other.receipt_handle == @receipt_handle
    end
  end
end
