
module Fluent
  class MapOutput < Fluent::Output
    Fluent::Plugin.register_output('map', self)

    config_param :map, :string, :default => nil
    config_param :tag, :string, :default => nil
    config_param :key, :string, :default => nil #deprected
    config_param :time, :string, :default => nil
    config_param :record, :string, :default => nil
    config_param :multi, :bool, :default => false
    config_param :timeout, :time, :default => 1

    def configure(conf)
      super
      if @map
        $log.debug { "map: #{@map}" }
        @mode = "tuple"
      elsif (@tag || @key) && @time && @record
        @tag ||= @key
        raise ConfigError, "multi and 3 parameters(tag, time, and record) are not compatible" if @multi
        $log.debug { "tag: #{@tag}, time: #{@time}, record: #{@record}" }
        @mode = "each"
      else
        raise ConfigError, "Either map or 3 parameters(key, time, and record) is required "
      end
    end

    def emit(tag, es, chain)
      begin
        tag_output_es = do_map(tag, es)
        tag_output_es.each_pair do |tag, output_es|
          Fluent::Engine::emit_stream(tag, output_es)
        end
        chain.next
        tag_output_es
      rescue SyntaxError => e
        chain.next
        $log.error "map command is syntax error: #{@map}"
        e #for test
      end
    end

    def do_map(tag, es)
      tuples = if @multi
                 generate_tuples_multi(tag, es)
               else
                 generate_tuples_single(tag, es)
               end
      tag_output_es = Hash.new{|h, key| h[key] = MultiEventStream::new}
      tuples.each do |tag, time, record|
        if time == nil || record == nil
          raise SyntaxError.new
        end
        tag_output_es[tag].add(time, record)
        $log.trace { [tag, time, record].inspect }
      end
      tag_output_es
    end

    def generate_tuples_multi(tag, es)
      tuples = []
      es.each {|time, record|
        new_tuple = eval(@map)
        tuples.concat new_tuple
      }
     tuples
    end

    def generate_tuples_single(tag, es)
      tuples = []
      es.each {|time, record|
        timeout_block(tag, time, record){
          case @mode
          when "tuple"
            new_tuple = eval(@map)
            tuples << new_tuple
          when "each"
            new_tag = eval(@tag)
            new_time = eval(@time)
            new_record = eval(@record)
            tuples << [new_tag, new_time, new_record]
          end
        }
      }
     tuples
    end

    def timeout_block(tag, time, record)
      begin
        Timeout.timeout(@timeout){
          yield
        }
      rescue Timeout::Error
        $log.error {"Timeout: #{Time.at(time)} #{tag} #{record.inspect}"}
      end
    end
  end
end
