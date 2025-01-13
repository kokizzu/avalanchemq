require "./exchange"

module LavinMQ
  module AMQP
    class HeadersExchange < Exchange
      @bindings = Hash(AMQP::Table, Set(Destination)).new do |h, k|
        h[k] = Set(Destination).new
      end

      def initialize(@vhost : VHost, @name : String, @durable = false,
                     @auto_delete = false, @internal = false,
                     @arguments = AMQP::Table.new)
        validate!(@arguments)
        super
      end

      def type : String
        "headers"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.flat_map do |args, ds|
          ds.map do |d|
            binding_key = BindingKey.new("", args)
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : Destination, routing_key, headers)
        validate!(headers)
        args = headers ? @arguments.clone.merge!(headers) : @arguments
        return false unless @bindings[args].add? destination
        binding_key = BindingKey.new(routing_key, args)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, headers)
        args = headers ? @arguments.clone.merge!(headers) : @arguments
        bds = @bindings[args]
        return false unless bds.delete(destination)
        @bindings.delete(routing_key) if bds.empty?

        binding_key = BindingKey.new(routing_key, args)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
      end

      protected def bindings(routing_key, headers) : Iterator(Destination)
        matches(headers).each
      end

      private def validate!(headers) : Nil
        if h = headers
          if match = h["x-match"]?
            if match != "all" && match != "any"
              raise LavinMQ::Error::PreconditionFailed.new("x-match must be 'any' or 'all'")
            end
          end
        end
      end

      private def matches(headers) : Iterator(Destination)
        @bindings.each.select do |args, _|
          if headers.nil? || headers.empty?
            args.empty?
          else
            case args["x-match"]?
            when "any"
              args.any? { |k, v| !k.starts_with?("x-") && (headers.has_key?(k) && headers[k] == v) }
            else
              args.all? { |k, v| k.starts_with?("x-") || (headers.has_key?(k) && headers[k] == v) }
            end
          end
        end.flat_map { |_, v| v.each }
      end
    end
  end
end