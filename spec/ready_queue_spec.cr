require "./spec_helper"

describe AvalancheMQ::Queue::ReadyQueue do
  it "should insert ordered" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    rq.insert(AvalancheMQ::SegmentPosition.new(0u32, 0u32))
    rq.insert(AvalancheMQ::SegmentPosition.new(1u32, 0u32))
    rq.insert(AvalancheMQ::SegmentPosition.new(0u32, 1u32))

    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 0u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 1u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(1u32, 0u32)
  end

  it "should insert array ordered" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    sps = Array(AvalancheMQ::SegmentPosition).new
    sps << AvalancheMQ::SegmentPosition.new(1u32, 0u32)
    sps << AvalancheMQ::SegmentPosition.new(0u32, 0u32)
    sps << AvalancheMQ::SegmentPosition.new(0u32, 1u32)
    rq.insert(sps)

    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 0u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(0u32, 1u32)
    rq.shift.should eq AvalancheMQ::SegmentPosition.new(1u32, 0u32)
  end

  it "should calculate message sizes" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    sps = [
      AvalancheMQ::SegmentPosition.new(10,10,5u32),
      AvalancheMQ::SegmentPosition.new(10,10,1u32),
      AvalancheMQ::SegmentPosition.new(10,10,10u32),
      AvalancheMQ::SegmentPosition.new(10,10,3u32),
      AvalancheMQ::SegmentPosition.new(10,10,1u32)
    ]
    sps.each { |sp| rq.insert(sp) }
    rq.sum(&.bytesize).should eq 20u32
    rq.max(&.bytesize).should eq 10u32
    rq.min(&.bytesize).should eq 1u32
    rq.avg(&.bytesize).should eq 4u32
  end

  it "should detect empty queue" do
    rq = AvalancheMQ::Queue::ReadyQueue.new
    rq.sum(&.bytesize).should eq 0u32
  end
end
