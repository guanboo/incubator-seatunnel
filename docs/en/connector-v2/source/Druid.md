# Druid

> Druid source connector

## Description

Read data from Apache Druid.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

:::tip

Reading data from Druid can also be done using JDBC

## Options

| name       | type           | required | default value |
| ---------- | -------------- | -------- | ------------- |
| url        | string       | yes      | -             |
| datasource | string       | yes      | -             |
| start_date | string       | no       | -             |
| end_date   | string       | no       | -             |
| columns    | List<string> | no       | *             |

### url [string]

`Druid` cluster broker address, the format is `jdbc:avatica:remote:url=http://host:port/druid/v2/sql/avatica/` ,Such as `"jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/"` .

### datasource [string]

The DataSource name in Apache Druid.

### start_date [string]

The start date of DataSource, for example, `'2016-06-27'`, `'2016-06-27 00:00:00'`, etc.

### end_date [string]

The end date of DataSource, for example, `'2016-06-28'`, `'2016-06-28 00:00:00'`, etc.

### columns [List<string>]

These columns that you want to write  of DataSource.

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](common-options.mdx) for details


## Example

```hocon
DruidSource {
  url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/"
  datasource = "wikipedia"
  start_date = "2016-06-27 00:00:00"
  end_date = "2016-06-28 00:00:00"
}
```

```hocon
DruidSource {
  url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/"
  datasource = "wikipedia"
  start_date = "2016-06-27 00:00:00"
  end_date = "2016-06-28 00:00:00"
  columns = ["flags","page"]
}
```
