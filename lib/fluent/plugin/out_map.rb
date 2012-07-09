
module Fluent
  class MapOutput < Fluent::Output
    Fluent::Plugin.register_output('map', self)

    config_param :map, :string
    config_param :multi, :bool, :default => false

    def emit(tag, es, chain)
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
        $log.trace { [tag, time, record].inspect }
        Fluent::Engine::emit(tag, time, record)
      end
      chain.next
      tuples
    end
  end
end
