
module Fluent
  class MapOutput < Fluent::Output
    Fluent::Plugin.register_output('map', self)

    config_param :map, :string
    config_param :multi, :bool, :default => false

    def configure(conf)
      super
      $log.debug { "map: #{@map}" }
    end

    def emit(tag, es, chain)
      begin
        tuples = []
        es.each {|time, record|
          new_tuple = eval(@map)
          if @multi
            tuples.concat new_tuple
          else
            tuples << new_tuple
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
        $log.error "Select_if command is syntax error: #{@map}"
        e #for test
      end
    end
  end
end
