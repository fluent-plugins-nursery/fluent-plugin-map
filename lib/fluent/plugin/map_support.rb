#
# fluent-plugin-map
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

module Fluent
  class MapSupport
    def initialize(map, plugin)
      @map = map
      @plugin = plugin
      if defined?(Fluent::Filter) and plugin.is_a?(Fluent::Filter)
        singleton_class.module_eval(<<-CODE)
          def map_func(time, record)
            #{@map}
          end
        CODE
        class << self
          alias_method :generate_tuples, :generate_tuples_filter
          alias_method :do_map, :do_map_filter
        end
      elsif plugin.is_a?(Fluent::Output)
        singleton_class.module_eval(<<-CODE)
          def map_func(tag, time, record)
            #{@map}
          end
        CODE
        class << self
          alias_method :generate_tuples, :generate_tuples_output
          alias_method :do_map, :do_map_output
        end
      end
    end

    def do_map(tag, es)
      # This method will be overwritten in #initailize.
    end

    def do_map_filter(tag, es)
      tuples = generate_tuples(tag, es)

      tag_output_es = Hash.new{|h, key| h[key] = Fluent::MultiEventStream.new}
      tuples.each do |time, record|
        if time == nil || record == nil
          raise SyntaxError.new
        end
        tag_output_es[tag].add(time, record)
        @plugin.log.trace { [tag, time, record].inspect }
      end
      tag_output_es
    end

    def do_map_output(tag, es)
      tuples = generate_tuples(tag, es)

      tag_output_es = Hash.new{|h, key| h[key] = Fluent::MultiEventStream.new}
      tuples.each do |tag, time, record|
        if time == nil || record == nil
          raise SyntaxError.new
        end
        tag_output_es[tag].add(time, record)
        @plugin.log.trace { [tag, time, record].inspect }
      end
      tag_output_es
    end

    def generate_tuples
      # This method will be overwritten in #initailize.
    end

    def generate_tuples_filter(tag, es)
      tuples = []
      es.each {|time, record|
        timeout_block do
          new_tuple = map_func(time, record)
          tuples.concat new_tuple
        end
      }
      tuples
    end

    def generate_tuples_output(tag, es)
      tuples = []
      es.each {|time, record|
        timeout_block do
          new_tuple = map_func(tag, time, record)
          tuples.concat new_tuple
        end
      }
      tuples
    end

    def timeout_block
      begin
        Timeout.timeout(@plugin.timeout){
          yield
        }
      rescue Timeout::Error
        @plugin.log.error {"Timeout: #{Time.at(time)} #{tag} #{record.inspect}"}
      end
    end
  end
end
