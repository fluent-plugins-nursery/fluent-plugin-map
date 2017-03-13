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
  module ParseMap
    module Mixin

      MMAP_MAX_NUM = 50

      def parse_map()
        if @multi
          @map
        else
          "[#{@map}]"
        end
      end

      def parse_multimap(conf)
        check_mmap_range(conf)

        prev_mmap = nil
        result_mmaps = (1..MMAP_MAX_NUM).map { |i|
          mmap = conf["mmap#{i}"]
          if (i > 1) && prev_mmap.nil? && !mmap.nil?
            raise Fluent::ConfigError, "Jump of mmap index found. mmap#{i - 1} is missing."
          end
          prev_mmap = mmap
          next if mmap.nil?

          mmap
        }.compact.join(',')
        "[#{result_mmaps}]"
      end

      def check_mmap_range(conf)
        invalid_mmap = conf.keys.select { |k|
          m = k.match(/^mmap(\d+)$/)
          m ? !((1..MMAP_MAX_NUM).include?(m[1].to_i)) : false
        }
        unless invalid_mmap.empty?
          raise Fluent::ConfigError, "Invalid mmapN found. N should be 1 - #{MMAP_MAX_NUM}: " + invalid_mmap.join(",")
        end
      end
    end
  end
end
