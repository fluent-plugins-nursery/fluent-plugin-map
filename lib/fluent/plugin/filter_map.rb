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

require 'fluent/plugin/map_support'
require 'fluent/plugin/map_config_param'

module Fluent
  class MapFilter < Fluent::Filter
    Fluent::Plugin.register_filter('map', self)

    include Fluent::MapConfigParam
    include Fluent::MapSupport

    def configure(conf)
      super
      @format = determine_format()
      configure_format()
      @map = create_map(conf)
      singleton_class.module_eval(<<-CODE)
        def map_func(time, record)
          #{@map}
        end
      CODE
    end

    def determine_format()
      if @format
        @format
      elsif @map
        "map"
      elsif @time && @record
        "record"
      else
        raise ConfigError, "Any of map, 2 parameters(time, and record) or format is required "
      end
    end

    def configure_format()
      case @format
      when "map"
        # pass
      when "record"
        raise ConfigError, "multi and 2 parameters(time, and record) are not compatible" if @multi
      when "multimap"
        # pass.
      else
        raise ConfigError, "format #{@format} is invalid."
      end
    end

    def create_map(conf)
      # return string like double array.
      case @format
      when "map"
        parse_map()
      when "record"
        "[[#{@time}, #{@record}]]"
      when "multimap"
        parse_multimap(conf)
      end
    end

    def do_map(tag, es)
      tuples = generate_tuples(tag, es)

      tag_output_es = Hash.new{|h, key| h[key] = MultiEventStream::new}
      tuples.each do |time, record|
        if time == nil || record == nil
          raise SyntaxError.new
        end
        tag_output_es[tag].add(time, record)
        log.trace { [tag, time, record].inspect }
      end
      tag_output_es
    end

    def generate_tuples(tag, es)
      tuples = []
      es.each {|time, record|
        timeout_block do
          new_tuple = map_func(time, record)
          tuples.concat new_tuple
        end
      }
      tuples
    end

    def filter_stream(tag, es)
      begin
        new_es = MultiEventStream.new
        tag_output_es = do_map(tag, es)
        tag_output_es.each_pair do |tag, output_es|
          output_es.each{|time, record|
            new_es.add(time, record)
          }
        end
        new_es
      rescue SyntaxError => e
        log.error "map command is syntax error: #{@map}"
        e #for test
      end
    end
  end
end
