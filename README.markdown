# fluent-plugin-map

[![Build Status](https://travis-ci.org/fluent-plugins-nursery/fluent-plugin-map.svg?branch=master)](https://travis-ci.org/fluent-plugins-nursery/fluent-plugin-map)

fluent-plugin-map(out\_map) is the non-buffered plugin that can convert an event log to different event log(s)

## Example

This sample config output code file and time file.

    <source>
      type tail
      format apache
      path /var/log/httpd-access.log
      tag tag
    </source>
    <match tag>
      type map
      map ([["code." + tag, time, {"code" => record["code"].to_i}], ["time." + tag, time, {"time" => record["time"].to_i}]])
      multi true
    </match>
    <match code.tag>
      type file
      path code.log
    </match>
    <match time.tag>
      type file
      path time.log
    </match>


The parameter "map" can use 3 variables in event log; tag, time, record. The format of time is an integer number of seconds since the Epoch. The format of record is hash.
The config file parses # as the begin of comment. So the "map" value cannot use #{tag} operation.
This plugin can output multi logs by seting multi to true.

If you don't use multi option, you can use key, time, record parameter. The 2 following match directive is same:

    <match tag>
      type map
      map (["code." + tag, time, {"code" => record["code"].to_i}])
    </match>
    <match tag>
      type map
      tag ("code." + tag)
      time time
      record ({"code" => record["code"].to_i})
    </match>

Note: you have to wrap some configuration values with parenthesis like `("code." + tag)`, to avoid parsing by Fluentd itself.
See also: [Fluentd | Configuration File | Format tips](http://docs.fluentd.org/articles/config-file#format-tips)

## Copyright

* Copyright (c) 2015- Tomita Kohei
* Apache License, Version 2.0
