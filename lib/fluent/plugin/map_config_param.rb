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
  module MapConfigParam
    def self.included(klass)
      klass.instance_eval {
        config_param :map, :string, :default => nil
        config_param :time, :string, :default => nil
        config_param :record, :string, :default => nil
        config_param :multi, :bool, :default => false
        config_param :timeout, :time, :default => 1
        config_param :format, :string, :default => nil
      }
    end
  end
end
