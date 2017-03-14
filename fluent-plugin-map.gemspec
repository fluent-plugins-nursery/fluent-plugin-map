# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)

Gem::Specification.new do |s|
  s.name        = "fluent-plugin-map"
  s.version     = "0.1.1"
  s.authors     = ["Kohei Tomita", "Hiroshi Hatake", "Kenji Okomoto"]
  s.email       = ["tommy.fmale@gmail.com", "cosmo0920.oucc@gmail.com", "okkez000@gmail.com"]
  s.homepage    = "https://github.com/fluent-plugins-nursery/fluent-plugin-map"
  s.summary     = %q{fluent-plugin-map is the non-buffered plugin that can convert an event log to different event log(s). }
  s.description = %q{fluent-plugin-map is the non-buffered plugin that can convert an event log to different event log(s).  }

  s.rubyforge_project = "fluent-plugin-map"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  s.license       = "Apache-2.0"

  s.add_development_dependency "rake"
  s.add_development_dependency "fluentd", [">= 0.10.24", "< 2"]
  s.add_development_dependency "test-unit", "~> 3.1"
  s.add_development_dependency "appraisal"
end
