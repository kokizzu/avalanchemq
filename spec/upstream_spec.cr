require "./spec_helper"
require "../src/lavinmq/federation/upstream"

module UpstreamSpecHelpers
  def self.setup_qs(ch) : {AMQP::Client::Exchange, AMQP::Client::Queue}
    x = ch.exchange("", "direct", passive: true)
    ch.queue("federation_q1")
    q2 = ch.queue("federation_q2")
    {x, q2}
  end

  def self.cleanup(upstream)
    links = upstream.links
    s.vhosts["/"].delete_queue("federation_q1")
    s.vhosts["/"].delete_queue("federation_q2")
    wait_for { links.all?(&.state.terminated?) }
  end

  def self.setup_ex_federation(upstream_name)
    upstream_vhost = s.vhosts.create("upstream")
    downstream_vhost = s.vhosts.create("downstream")
    upstream = LavinMQ::Federation::Upstream.new(downstream_vhost, upstream_name,
      "#{AMQP_BASE_URL}/upstream", "upstream_ex")
    upstream.ack_timeout = 1.milliseconds
    downstream_vhost.upstreams.not_nil!.add(upstream)
    {upstream, upstream_vhost, downstream_vhost}
  end

  def self.start_link(upstream)
    definitions = {"federation-upstream" => JSON::Any.new(upstream.name)} of String => JSON::Any
    upstream.vhost.add_policy("FE", "downstream_ex", "exchanges",
      definitions, 12_i8)
  end

  def self.cleanup_ex_federation
    v1 = s.vhosts["downstream"]
    v2 = s.vhosts["upstream"]
    s.vhosts.delete("downstream")
    s.vhosts.delete("upstream")

    wait_for { !(Dir.exists?(v1.data_dir) || Dir.exists?(v2.data_dir)) }
  end
end

describe LavinMQ::Federation::Upstream do
  it "should federate queue" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup(upstream.not_nil!)
  end

  it "should not federate queue if no downstream consumer" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream wo downstream", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x = UpstreamSpecHelpers.setup_qs(ch).first
      x.publish "federate me", "federation_q1"
      link = upstream.link(vhost.queues["federation_q2"])
      wait_for { link.state.running? }
      vhost.queues["federation_q1"].message_count.should eq 1
      vhost.queues["federation_q2"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup(upstream.not_nil!)
  end

  it "should federate queue with ack mode no-ack" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream no-ack", AMQP_BASE_URL, nil, "federation_q1",
      ack_mode: LavinMQ::Federation::AckMode::NoAck)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup(upstream.not_nil!)
  end

  it "should federate queue with ack mode on-publish" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream on-publish", AMQP_BASE_URL, nil, "federation_q1",
      ack_mode: LavinMQ::Federation::AckMode::OnPublish)

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup(upstream.not_nil!)
  end

  it "should resume federation after downstream reconnects" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream reconnect", AMQP_BASE_URL, nil, "federation_q1")
    msgs = [] of AMQP::Client::DeliverMessage

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      upstream.link(vhost.queues["federation_q2"])
      q2.subscribe do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1"
      q2.subscribe do |msg|
        msgs << msg
      end
      wait_for { msgs.size == 2 }
      msgs.size.should eq 2
      vhost.queues["federation_q1"].message_count.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup(upstream.not_nil!)
  end

  it "should federate exchange" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "ef test upstream", AMQP_BASE_URL, "upstream_ex")

    with_channel do |ch|
      downstream_ex = ch.exchange("downstream_ex", "topic")
      downstream_q = ch.queue("downstream_q")
      downstream_q.bind(downstream_ex.name, "#")
      link = upstream.link(vhost.exchanges[downstream_ex.name])
      wait_for { link.state.running? }
      upstream_ex = ch.exchange("upstream_ex", "topic", passive: true)
      upstream_ex.publish "federate me", "rk"
      msgs = [] of AMQP::Client::DeliverMessage
      downstream_q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  ensure
    s.vhosts["/"].delete_queue("downstream_q")
    s.vhosts["/"].delete_exchange("downstream_ex")
    s.vhosts["/"].delete_exchange("upstream_ex")
    wait_for { upstream.not_nil!.links.all?(&.state.terminated?) }
  end

  it "should keep message properties" do
    vhost = s.vhosts["/"]
    upstream = LavinMQ::Federation::Upstream.new(vhost, "qf test upstream props", AMQP_BASE_URL, nil, "federation_q1")

    with_channel do |ch|
      x, q2 = UpstreamSpecHelpers.setup_qs ch
      x.publish "federate me", "federation_q1", props: AMQP::Client::Properties.new(content_type: "application/json")
      upstream.link(vhost.queues["federation_q2"])
      msgs = [] of AMQP::Client::DeliverMessage
      q2.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.first.properties.content_type.should eq "application/json"
    end
  ensure
    UpstreamSpecHelpers.cleanup(upstream.not_nil!)
  end

  it "should federate exchange even with no downstream consumer" do
    upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_ex_federation("ef test upstream wo downstream")
    UpstreamSpecHelpers.start_link(upstream)
    s.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    s.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)
    with_channel(vhost: "upstream") do |upstream_ch|
      with_channel(vhost: "downstream") do |downstream_ch|
        upstream_ex = upstream_ch.exchange("upstream_ex", "topic")
        downstream_ch.exchange("downstream_ex", "topic")
        wait_for { downstream_vhost.exchanges["downstream_ex"].policy.try(&.name) == "FE" }
        # Assert setup is correct
        wait_for { upstream.links.first?.try &.state.running? }
        downstream_q = downstream_ch.queue("downstream_q")
        downstream_q.bind("downstream_ex", "#")
        upstream_ex.publish_confirm "federate me", "rk"
        wait_for { downstream_q.get }
        msgs = [] of AMQP::Client::DeliverMessage
        downstream_q.subscribe { |msg| msgs << msg }
        upstream_ex.publish_confirm "federate me", "rk"
        wait_for { msgs.size == 1 }
        msgs.first.not_nil!.body_io.to_s.should eq("federate me")
      end
    end
    upstream_vhost.queues.each_value.all?(&.empty?).should be_true
  ensure
    UpstreamSpecHelpers.cleanup_ex_federation
  end

  it "should continue after upstream restart" do
    upstream, upstream_vhost, downstream_vhost = UpstreamSpecHelpers.setup_ex_federation("ef test upstream restart")
    UpstreamSpecHelpers.start_link(upstream)
    s.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    s.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

    with_channel(vhost: "upstream") do |upstream_ch|
      with_channel(vhost: "downstream") do |downstream_ch|
        upstream_ex = upstream_ch.exchange("upstream_ex", "topic")
        downstream_ch.exchange("downstream_ex", "topic")
        wait_for { downstream_vhost.exchanges["downstream_ex"].policy.try(&.name) == "FE" }
        # Assert setup is correct
        wait_for { upstream.links.first?.try &.state.running? }
        downstream_q = downstream_ch.queue("downstream_q")
        downstream_q.bind("downstream_ex", "#")
        msgs = [] of AMQP::Client::DeliverMessage
        downstream_q.subscribe do |msg|
          msgs << msg
        end
        upstream_ex.publish_confirm "federate me", "rk1"
        wait_for { msgs.size == 1 }
        upstream_vhost.connections.each do |conn|
          next unless conn.client_name.starts_with?("Federation link")
          conn.close
        end
        wait_for { upstream.links.first?.try { |l| l.state.stopped? || l.state.starting? } }
        upstream_ex.publish_confirm "federate me", "rk2"
        # Should reconnect
        wait_for { upstream.links.first?.try(&.state.running?) }
        upstream_ex.publish_confirm "federate me", "rk3"
        wait_for {
          msgs.size == 3
        }
      end
    end
    upstream_vhost.queues.each_value.all?(&.empty?).should be_true
  ensure
    UpstreamSpecHelpers.cleanup_ex_federation
  end

  it "should reflect all bindings to upstream q" do
    upstream, upstream_vhost, _ = UpstreamSpecHelpers.setup_ex_federation("ef test bindings")
    s.users.add_permission("guest", "upstream", /.*/, /.*/, /.*/)
    s.users.add_permission("guest", "downstream", /.*/, /.*/, /.*/)

    with_channel(vhost: "downstream") do |downstream_ch|
      downstream_ch.exchange("downstream_ex", "topic")
      queues = [] of AMQP::Client::Queue
      10.times do |i|
        downstream_q = downstream_ch.queue("")
        downstream_q.bind("downstream_ex", "before.link.#{i}")
        queues << downstream_q
      end

      UpstreamSpecHelpers.start_link(upstream)
      wait_for { upstream.links.first?.try &.state.running? }

      upstream_q = upstream_vhost.queues.values.first
      upstream_q.bindings.size.should eq queues.size
      # Assert setup is correct
      10.times do |i|
        downstream_q = downstream_ch.queue("")
        downstream_q.bind("downstream_ex", "after.link.#{i}")
        queues << downstream_q
      end
      sleep 0.1
      upstream_q.bindings.size.should eq queues.size
      queues.each &.delete
      sleep 0.01
      upstream_q.bindings.size.should eq 0
    end
  ensure
    UpstreamSpecHelpers.cleanup_ex_federation
  end
end
