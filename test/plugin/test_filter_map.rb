require 'helper'
begin
  require 'fluent/test/driver/filter'
  require 'fluent/test/helpers'
  include Fluent::Test::Helpers
rescue LoadError
end

class MapFilterTest < Test::Unit::TestCase
  def setup
    omit "Use Fluentd v0.14 or later" unless defined?(Fluent::Plugin::Filter)
    Fluent::Test.setup
  end

  CONFIG = %[
    map ([time, record])
    multi false
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Filter.new(Fluent::Plugin::MapFilter).configure(conf)
  end

  def test_syntax_error
    tag = "tag"
    time = event_time('2012-10-10 10:10:10')
    record = {'code' => '300'}

    #map is syntax error
    syntax_error_config = %[
      map time.
    ]
    assert_raise SyntaxError do
      create_driver(syntax_error_config)
    end
  end

  def test_syntax_error2
    tag = "tag"
    time = event_time('2012-10-10 10:10:10')
    record = {'code' => '300'}

    #map output lligal format
    syntax_error_config = %[
      map time
    ]
    d1 = create_driver(syntax_error_config)
    es = Fluent::OneEventStream.new(time, record)
    e =  d1.instance.filter_stream(tag, es)
    assert e.kind_of?(SyntaxError)
  end

  def test_tag_convert_using_time_record
    tag = 'tag.raw'
    time = event_time('2012-10-10 10:10:10')
    record = {'code' => '300'}

    d1 = create_driver %[
      time time
      record record
    ]
    d1.run(default_tag: tag) do
      d1.feed(time, record)
    end
    filtered = d1.filtered
    assert_equal 1, filtered.length
    assert_equal [time, record], filtered[0]
  end

  #deprected specification test
  def test_config_error_time
    tag = "tag"
    record = {'code' => '300'}

    #require time
    assert_raise(Fluent::ConfigError){
      create_driver %[
        record record
      ]
    }
  end

  def test_config_error_record
    time = event_time('2012-10-10 10:10:10')

    #require record
    assert_raise(Fluent::ConfigError){
      create_driver %[
        time time
      ]
    }
  end

  def test_config_error_multi
    time = event_time('2012-10-10 10:10:10')
    record = {'code' => '300'}

    #require time
    assert_raise(Fluent::ConfigError){
      create_driver %[
        time time
        record record
        multi true
      ]
    }
  end

  def test_config_error_sleep
    time = event_time('2012-10-10 10:10:10')
    record = {'code' => '300'}

    assert_raise(SyntaxError) {
      create_driver %[
        tag "newtag"
        time sleep 10
        record record
        timeout 1s
      ]
    }
  end

  # Add format test
  ## test format type (map, record, maps)
  ## test Backward compatibility without format
  ##

  def test_convert_format_map
    tag = 'tag.raw'
    time = event_time('2012-10-10 10:10:10')
    record = {'code1' => '300', 'code2' => '400'}

    d1 = create_driver %[
      format map
      map ([[time, record["code1"]], [time, record["code2"]]])
      multi true
    ]
    d1.run(default_tag: tag) do
      d1.feed(time, record)
    end
    filtered = d1.filtered
    assert_equal 2, filtered.length
    assert_equal [time, record["code1"]], filtered[0]
    assert_equal [time, record["code2"]], filtered[1]
  end

  def test_tag_convert_format_record
    tag = 'tag.raw'
    time = event_time('2012-10-10 10:10:10')
    record = {'code' => '300'}

    d1 = create_driver %[
      format record
      time time
      record record
    ]
    d1.run(default_tag: tag) do
      d1.feed(time, record)
    end
    filtered = d1.filtered
    assert_equal 1, filtered.length
    assert_equal [time, record], filtered[0]
  end

  def test_convert_format_multimap
    tag = 'tag.raw'
    time =  event_time('2012-10-10 10:10:10')
    record = {'code1' => '300', 'code2' => '400'}

    d1 = create_driver %[
      format multimap
      mmap1 ([time, record["code1"]])
      mmap2 ([time, record["code2"]])
    ]
    d1.run(default_tag: tag) do
      d1.feed(time, record)
    end
    filtered = d1.filtered
    assert_equal 2, filtered.length
    assert_equal [time, record["code1"]], filtered[0]
    assert_equal [time, record["code2"]], filtered[1]
  end
end
