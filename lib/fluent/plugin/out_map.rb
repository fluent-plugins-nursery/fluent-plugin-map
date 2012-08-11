
module Fluent
  class MapOutput < Fluent::Output
    Fluent::Plugin.register_output('map', self)

    config_param :map, :string, :default => nil
    config_param :key, :string, :default => nil
    config_param :time, :string, :default => nil
    config_param :record, :string, :default => nil
    config_param :multi, :bool, :default => false

    def configure(conf)
      super
      if @map
        $log.debug { "map: #{@map}" }
        @mode = "tuple"
      elsif @key && @time && @record
        raise ConfigError, "multi and 3 parameters(key, time, and record) are not compatible" if @multi
        $log.debug { "key: #{@key}, time: #{@time}, record: #{@record}" }
        @mode = "each"
      else
        raise ConfigError, "Either map or 3 parameters(key, time, and record) is required "
      end
    end

    def emit(tag, es, chain)
      begin
        tuples = []
        es.each {|time, record|
          case @mode
          when "tuple"
            new_tuple = eval(@map)
            if @multi
              tuples.concat new_tuple
            else
              tuples << new_tuple
            end
          when "each"
            new_key = eval(@key)
            new_time = eval(@time)
            new_record = eval(@record)
            tuples << [new_key, new_time, new_record]
          end
        }
        tuples.each do |tag, time, record|
          if time == nil or record == nil
            raise SyntaxError.new
          end
          $log.trace { [tag, time, record].inspect }
          Fluent::Engine::emit(tag, time, record)
        end
        chain.next
        tuples
      rescue SyntaxError => e
        chain.next
        $log.error "map command is syntax error: #{@map}"
        e #for test
      end
    end
  end
end
