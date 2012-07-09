# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "fluent-plugin-map"
  s.version     = "0.0.1"
  s.authors     = ["Kohei Tomita"]
  s.email       = ["tommy.fmale@gmail.com"]
  s.homepage    = ""
  s.summary     = %q{fluent-plugin-map is the non-buffered plugin that can convert an event log to different event log(s). }
  s.description = %q{fluent-plugin-map is the non-buffered plugin that can convert an event log to different event log(s).  }

  s.rubyforge_project = "fluent-plugin-map"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # specify any dependencies here; for example:
  s.add_development_dependency "rake"
  s.add_development_dependency "fluentd"
  # s.add_runtime_dependency "rest-client"
end
