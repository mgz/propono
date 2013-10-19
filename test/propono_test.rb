require File.expand_path('../test_helper', __FILE__)

module Propono
  class ProponoTest < Minitest::Test

    def test_publish_calls_publisher_publish
      topic, message = "Foo", "Bar"
      Publisher.expects(:publish).with(topic, message, {})
      Propono.publish(topic, message)
    end

    def test_subscribe_by_queue_calls_subscribe
      topic = 'foobar'
      Subscriber.expects(:subscribe_by_queue).with(topic)
      Propono.subscribe_by_queue(topic)
    end

    def test_subscribe_by_post_calls_subscribe
      topic, endpoint = 'foo', 'bar'
      Subscriber.expects(:subscribe_by_post).with(topic, endpoint)
      Propono.subscribe_by_post(topic, endpoint)
    end

    def test_listen_to_queue_calls_queue_listener
      topic = 'foobar'
      QueueListener.expects(:listen).with(topic)
      Propono.listen_to_queue(topic)
    end

    def test_listen_to_udp_calls_udp_listener
      UdpListener.expects(:listen).with()
      Propono.listen_to_udp()
    end

    def test_proxy_udp_calls_listen
      UdpListener.expects(:listen).with()
      Propono.proxy_udp("foobar")
    end

    def test_proxy_udp_calls_publish_in_the_block
      topic = "foobar"
      message = "message"
      Propono.stubs(:listen_to_udp).yields(message)
      Publisher.expects(:publish).with(topic, message, {})
      Propono.proxy_udp(topic)
    end
  end
end
