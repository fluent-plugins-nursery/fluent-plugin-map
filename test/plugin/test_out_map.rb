require 'helper'

class MapOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    map [tag, time, record]
    multi false
  ]

  def create_driver(conf = CONFIG, tag='test.input')
    Fluent::Test::OutputTestDriver.new(Fluent::MapOutput, tag).configure(conf)
  end

  def test_tag_convert
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code' => '300'}

    d1 = create_driver %[
      map ["newtag", time, record]
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 1, emits.length
    assert_equal ["newtag", time, record], emits[0]
  end

  def test_convert_multi_tag
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code' => '300'}

    d1 = create_driver %[
      map [["tag1", time, record], ["tag2", time, record]]
      multi true
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 2, emits.length
    assert_equal ["tag1", time, record], emits[0]
    assert_equal ["tag2", time, record], emits[1]
  end

  def test_syntax_error
    tag = "tag"
    time = Time.local(2012, 10, 10, 10, 10, 0).to_i
    record = {'code' => '300'}

    #map is syntax error
    syntax_error_config = %[
      map tag.
    ]
    assert_raise SyntaxError do
      d1 = create_driver(syntax_error_config, tag)
    end
  end

  def test_syntax_error2
    tag = "tag"
    time = Time.local(2012, 10, 10, 10, 10, 0).to_i
    record = {'code' => '300'}

    #map output lligal format
    syntax_error_config = %[
      map tag
    ]
    d1 = create_driver(syntax_error_config, tag)
    es = Fluent::OneEventStream.new(time, record)
    chain = Fluent::Test::TestOutputChain.new
    e =  d1.instance.emit(tag, es, chain)
    assert e.kind_of?(SyntaxError)
  end

  def test_tag_convert_using_tag_time_record
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code' => '300'}

    d1 = create_driver %[
      tag "newtag"
      time time
      record record
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 1, emits.length
    assert_equal ["newtag", time, record], emits[0]
  end

  #deprected specification test
  def test_tag_convert_using_key_time_record
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code' => '300'}

    d1 = create_driver %[
      key "newtag"
      time time
      record record
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 1, emits.length
    assert_equal ["newtag", time, record], emits[0]
  end

  def test_config_error_tag
    tag = "tag"
    time = Time.local(2012, 10, 10, 10, 10, 0).to_i
    record = {'code' => '300'}

    #require time
    assert_raise(Fluent::ConfigError){
      create_driver %[
        time time
        record record
      ], tag
    }
  end

  def test_config_error_time
    tag = "tag"
    record = {'code' => '300'}

    #require time
    assert_raise(Fluent::ConfigError){
      create_driver %[
        tag "newtag"
        record record
      ], tag
    }
  end

  def test_config_error_record
    tag = "tag"
    time = Time.local(2012, 10, 10, 10, 10, 0).to_i

    #require record
    assert_raise(Fluent::ConfigError){
      create_driver %[
        tag "newtag"
        time time
      ], tag
    }
  end

  def test_config_error_multi
    tag = "tag"
    time = Time.local(2012, 10, 10, 10, 10, 0).to_i
    record = {'code' => '300'}

    #require time
    assert_raise(Fluent::ConfigError){
      create_driver %[
        tag "newtag"
        time time
        record record
        multi true
      ], tag
    }
  end

  def test_config_error_sleep
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code' => '300'}

    assert_raise(SyntaxError) {
      create_driver %[
        key "newtag"
        time sleep 10
        record record
        timeout 1s
      ], tag
    }
  end

  # Add format test
  ## test format type (map, record, maps)
  ## test Backward compatibility without format
  ##

  def test_convert_format_map
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code1' => '300', 'code2' => '400'}

    d1 = create_driver %[
      format map
      map [["tag1", time, record["code1"]], ["tag2", time, record["code2"]]]
      multi true
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 2, emits.length
    assert_equal ["tag1", time, record["code1"]], emits[0]
    assert_equal ["tag2", time, record["code2"]], emits[1]
  end

  def test_tag_convert_format_record
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code' => '300'}

    d1 = create_driver %[
      format record
      tag "newtag"
      time time
      record record
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 1, emits.length
    assert_equal ["newtag", time, record], emits[0]
  end

  def test_convert_format_multimap
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10).to_i
    record = {'code1' => '300', 'code2' => '400'}

    d1 = create_driver %[
      format multimap
      mmap1 ["tag1", time, record["code1"]]
      mmap2 ["tag2", time, record["code2"]]
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 2, emits.length
    assert_equal ["tag1", time, record["code1"]], emits[0]
    assert_equal ["tag2", time, record["code2"]], emits[1]
  end
end
