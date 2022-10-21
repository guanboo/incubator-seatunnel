# Druid

> Druid sink connector

## Description

Used to write data to Druid.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

The Druid sink plug-in can achieve accuracy once by implementing idempotent writing, and needs to cooperate with aggregatingmergetree and other engines that support deduplication.

- [ ] [schema projection](../../concept/connector-v2-features.md)


:::

## Options

| name                    | type     | required | default value |
| ----------------------- | -------- | -------- | ------------- |
| coordinator_url         | string | yes      | -             |
| datasource              | string | yes      | -             |
| columns                 | List<string> | yes| -             |
| timestamp_column        | string | no       | timestamp     |
| timestamp_format        | string | no       | auto          |
| timestamp_missing_value | string| no       | -             |

### coordinator_url [string]

`Druid` cluster coordinator address, the format is `host:port` ,Such as `"host:8081"` .

### datasource [string]

The `Druid` datasource

### columns [array]

The data field that needs to be output to `Druid` , if not configured, it will be automatically adapted according to the sink table `schema` .

### timestamp_column [string]

The timestamp column name in Apache Druid, the default value is `timestamp`.

### timestamp_format [string]

The timestamp format in Apache Druid, the default value is `auto`, it could be:

- `iso`
  - ISO8601 with 'T' separator, like "2000-01-01T01:02:03.456"

- `posix`
  - seconds since epoch

- `millis`
  - milliseconds since epoch

- `micro`
  - microseconds since epoch

- `nano`
  - nanoseconds since epoch

- `auto`
  - automatically detects ISO (either 'T' or space separator) or millis format

- any [Joda DateTimeFormat](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) string

### timestamp_missing_value [string]

The timestamp missing value in Apache Druid, which is used for input records that have a null or missing timestamp. The value of `timestamp_missing_value` should be in ISO 8601 format, for example `"2022-02-02T02:02:02.222"`.

### Simple

```hocon
DruidSink {
  coordinator_url = "http://localhost:8081/"
  datasource = "wikipedia"
  columns = ["flags","page"]
}
```

### Specified timestamp column and format

```hocon
DruidSink {
  coordinator_url = "http://localhost:8081/"
  datasource = "wikipedia"
  timestamp_column = "timestamp"
  timestamp_format = "auto"
  columns = ["flags","page"]
}
```

### Specified timestamp column, format and missing value

```hocon
DruidSink {
  coordinator_url = "http://localhost:8081/"
  datasource = "wikipedia"
  timestamp_column = "timestamp"
  timestamp_format = "auto"
  timestamp_missing_value = "2022-02-02T02:02:02.222"
  columns = ["flags","page"]
}
```
