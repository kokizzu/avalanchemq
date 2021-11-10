require "./spec_helper"

describe AvalancheMQ::Queue::UnackQueue do
  it "should calculate message sizes" do
    uq = AvalancheMQ::Queue::UnackQueue.new
    sps = [
      AvalancheMQ::SegmentPosition.new(10,10,5u32),
      AvalancheMQ::SegmentPosition.new(10,10,1u32),
      AvalancheMQ::SegmentPosition.new(10,10,10u32),
      AvalancheMQ::SegmentPosition.new(10,10,3u32),
      AvalancheMQ::SegmentPosition.new(10,10,1u32)
    ]
    sps.each { |sp| uq.push(sp, nil) }
    uq.sum(&.sp.bytesize).should eq 20u32
    uq.max(&.sp.bytesize).should eq 10u32
    uq.min(&.sp.bytesize).should eq 1u32
    uq.avg(&.sp.bytesize).should eq 4u32
  end

  it "should detect empty queue" do
    uq = AvalancheMQ::Queue::UnackQueue.new
    uq.sum(&.sp.bytesize).should eq 0u32
  end
end
