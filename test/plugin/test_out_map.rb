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
    time = Time.local(2012, 10, 10, 10, 10, 10)
    record = {'code' => '300'}

    d1 = create_driver %[
      map ["newtag", time, record]
    ], tag
    d1.run do
      d1.emit(record, time)
    end
    emits = d1.emits
    assert_equal 1, emits.length
    assert_equal ["newtag", time.to_i, record], emits[0]
  end

  def test_convert_multi_tag
    tag = 'tag'
    time = Time.local(2012, 10, 10, 10, 10, 10)
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
    assert_equal ["tag1", time.to_i, record], emits[0]
    assert_equal ["tag2", time.to_i, record], emits[1]
  end

  def test_syntax_error
    tag = "tag"
    time = Time.local(2012, 10, 10, 10, 10, 0)
    record = {'code' => '300'}

    #map is syntax error
    syntax_error_config = %[
      map tag.
    ]
    d1 = create_driver(syntax_error_config, tag)
    es = Fluent::OneEventStream.new(time.to_i, record)
    chain = Fluent::Test::TestOutputChain.new
    e =  d1.instance.emit(tag, es, chain)
    assert e.kind_of?(SyntaxError)
  end
end
