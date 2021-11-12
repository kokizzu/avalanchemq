require "../segment_position"
require "../client/channel/consumer"

module AvalancheMQ
  class Queue
    class UnackQueue
      record Unack,
        sp : SegmentPosition,
        consumer : Client::Channel::Consumer?

      @lock = Mutex.new(:checked)

      def initialize(capacity = 8)
        @unacked = Deque(Unack).new(capacity)
      end

      def push(sp : SegmentPosition, consumer : Client::Channel::Consumer?)
        @lock.synchronize do
          unacked = @unacked
          unack = Unack.new(sp, consumer)
          if idx = unacked.bsearch_index { |u| u.sp > sp }
            unacked.insert(idx, unack)
          else
            unacked << unack
          end
        end
      end

      def delete(sp : SegmentPosition) : Nil
        @lock.synchronize do
          unacked = @unacked
          if idx = unacked.bsearch_index { |u| u.sp >= sp }
            if unacked[idx].sp == sp
              unacked.delete_at(idx)
              compact
            end
          end
        end
      end

      def delete(consumer : Client::Channel::Consumer) : Array(SegmentPosition)
        consumer_unacked = Array(SegmentPosition).new(Math.max(consumer.prefetch_count, 16))
        @lock.synchronize do
          @unacked.reject! do |unack|
            if unack.consumer == consumer
              consumer_unacked << unack.sp
              true
            end
          end
          compact
        end
        consumer_unacked
      end

      def sum(&blk : Unack -> _) : UInt64
        @unacked.sum(0_u64, &blk)
      end

      def size
        @unacked.size
      end

      def capacity
        @unacked.capacity
      end

      def locked_each
        @lock.synchronize do
          yield @unacked.each
        end
      end

      def each_sp(&blk)
        @lock.synchronize do
          @unacked.each { |unack| yield unack.sp }
        end
      end

      private def compact : Nil
        unacked = @unacked
        return unless unacked.capacity > unacked.size + 2**17 # when there's 3MB free in the deque
        {% unless flag?(:release) %}
          puts "compacting internal unacked queue capacity=#{unacked.capacity} size=#{unacked.size}"
        {% end %}
        @unacked = unacked.dup
      end

      def purge
        @lock.synchronize do
          s = @unacked.size
          @unacked.clear
          s
        end
      end
    end
  end
end
