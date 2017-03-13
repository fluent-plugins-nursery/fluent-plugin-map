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
require 'fluent/plugin/parse_map_mixin'

module Fluent
  class MapOutput < Fluent::Output
    Fluent::Plugin.register_output('map', self)

    include Fluent::MapConfigParam
    include Fluent::ParseMap::Mixin

    # Define `router` method of v0.12 to support v0.10 or earlier
    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    unless method_defined?(:log)
      define_method("log") { $log }
    end

    config_param :key, :string, :default => nil #deprecated
    config_param :tag, :string, :default => nil

    def configure(conf)
      super
      @format = determine_format()
      configure_format()
      @map = create_map(conf)
      @map_support = Fluent::MapSupport.new(@map, self)
    end

    def determine_format()
      if @format
        @format
      elsif @map
        "map"
      elsif (@tag || @key) && @time && @record
        "record"
      else
        raise Fluent::ConfigError, "Any of map, 3 parameters(tag, time, and record) or format is required "
      end
    end

    def configure_format()
      case @format
      when "map"
        # pass
      when "record"
        @tag ||= @key
        raise Fluent::ConfigError, "multi and 3 parameters(tag, time, and record) are not compatible" if @multi
      when "multimap"
        # pass.
      else
        raise Fluent::ConfigError, "format #{@format} is invalid."
      end
    end

    def create_map(conf)
      # return string like double array.
      case @format
      when "map"
        parse_map()
      when "record"
        "[[#{@tag}, #{@time}, #{@record}]]"
      when "multimap"
        parse_multimap(conf)
      end
    end

    def emit(tag, es, chain)
      begin
        tag_output_es = @map_support.do_map(tag, es)
        tag_output_es.each_pair do |tag, output_es|
          router.emit_stream(tag, output_es)
        end
        chain.next
        tag_output_es
      rescue SyntaxError => e
        chain.next
        log.error "map command is syntax error: #{@map}"
        e #for test
      end
    end
  end
end
