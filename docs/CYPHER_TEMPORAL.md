# Cypher temporal types

HawDB implements the openCypher temporal types as native `Value` variants
built on [chrono](https://docs.rs/chrono) 0.4 (issue #1361).

## Types

| Cypher type | `Value` variant | chrono type | Notes |
| --- | --- | --- | --- |
| `DATE` | `Value::Date` | `NaiveDate` | proleptic Gregorian, years ±262143 |
| `LOCAL TIME` | `Value::LocalTime` | `NaiveTime` | nanosecond precision, no leap seconds |
| `LOCAL DATETIME` | `Value::LocalDateTime` | `NaiveDateTime` | |
| `ZONED DATETIME` | `Value::DateTime` | `DateTime<FixedOffset>` | fixed UTC offset only |
| `DURATION` | `Value::Duration` | `hawdb_core::temporal::Duration` (`TemporalDuration`) | `months`, `days`, exact `time: TimeDelta` |

A duration keeps calendar months and days separate from its exact time part:
one month or one day has no fixed length. `ZONED TIME` (time with an offset)
and named time-zone regions are not supported. The host API exposes the module
as `hawdb::temporal` (parsers, formatters, the statement clock and the binary
payload codec).

## Ordering, equality and comparison

* New kinds rank after all existing kinds in `Value`'s total order, in the
  order date, local time, local datetime, zoned datetime, duration.
* Within a kind, values order chronologically. Zoned datetimes order by UTC
  instant and then by offset, so equal instants in different offsets are
  *different* values (`=` is false, as in openCypher) and `Ord`, `Eq` and
  `Hash` agree. `<`/`>` therefore order two such values by offset.
* Durations have a total internal order `(months, days, time)` for sorting,
  grouping, `DISTINCT` and indexes. Cypher `<`, `<=`, `>`, `>=` between two
  durations return `NULL`; `=` compares components (`P1D <> PT24H`). This
  holds for every operand form (property, parameter, folded literal or
  general expression) and in storage filters, so `NOT n.d < $limit` is
  `NULL` too and selects no rows.
* Comparing different temporal kinds, or a temporal value with a string or
  number, is never true (HawDB's existing mixed-type rule; openCypher returns
  `NULL`).
* `ORDER BY`, `min`, `max`, `collect`, `count` and `DISTINCT` work on all kinds.

## Functions

| Function | Behaviour |
| --- | --- |
| `date()`, `localtime()`, `localdatetime()`, `datetime()` | Statement clock (UTC). |
| `date(x)` … `datetime(x)` | `x` is an ISO 8601 string, a map, another temporal value (conversion) or `NULL`. |
| `datetime({epochMillis: n})`, `datetime({epochSeconds: n[, nanosecond: n]})` | UTC unless `timezone` is given. |
| `datetime.fromepoch(s, ns)`, `datetime.fromepochmillis(ms)` | UTC. |
| `duration('P1Y2M3DT4H5M6.5S')`, `duration({days: 3, hours: 2})` | Components may be negative or fractional; fractions cascade (a month is 30.436875 days). Time does not carry into days (`PT25H` stays 25 hours). |
| `duration.between(a, b)` | Months, then days, then the exact remainder. |
| `duration.inMonths/inDays/inSeconds(a, b)` | Whole months, whole days, exact time. |
| `date/localtime/localdatetime/datetime.truncate(unit, x)` | Units `millennium`, `century`, `decade`, `year`, `weekYear`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `millisecond`, `microsecond`. |

Accepted strings: dates `YYYY-MM-DD`, `YYYYMMDD`, `YYYY-MM`, `YYYY`,
`YYYY-Www-D`, `YYYY-DDD` (signed for years outside 0000–9999); times
`HH:MM[:SS[.fffffffff]]` and `HHMM[SS]`; offsets `Z`, `±HH`, `±HHMM`,
`±HH:MM[:SS]`. A datetime without an offset is UTC. Map constructors accept
`year`/`month`/`day`, `week`/`dayOfWeek`, `ordinalDay`, `quarter`/`dayOfQuarter`,
`hour`…`nanosecond`, base values `date`, `time`, `datetime`, and `timezone`.

### Component access

Property syntax works on any expression (`p.birthday.month`,
`duration.between(a, b).minutes`, `$map.key`):

* instants: `year`, `quarter`, `month`, `week`, `weekYear`, `day`,
  `ordinalDay` (alias `dayOfYear`), `dayOfQuarter`, `dayOfWeek` (alias
  `weekDay`, Monday = 1), `hour`, `minute`, `second`, `millisecond`,
  `microsecond`, `nanosecond`; zoned datetimes add `timezone`/`offset`
  (`"+02:00"`), `offsetMinutes`, `offsetSeconds`, `epochSeconds`, `epochMillis`;
* durations: `years`, `quarters`, `months`, `weeks`, `days`, `hours`,
  `minutes`, `seconds`, `milliseconds`, `microseconds`, `nanoseconds` and the
  `…Of…` forms (`monthsOfYear`, `minutesOfHour`, `nanosecondsOfSecond`, …).
  As in Neo4j, `hours`/`minutes`/`seconds` read only the time part:
  `duration.between(a, b).minutes` ignores whole months and days; use
  `duration.inSeconds(a, b).minutes` for total elapsed minutes.

Unknown components are errors. On maps, `.key` reads the entry.

### Arithmetic

`instant ± duration` applies months (clamping the day of month), then days,
then the exact time. Dates ignore the sub-day remainder of the time part;
local times ignore months and days and wrap at midnight. `duration ± duration`
is component-wise; `duration * number`, `number * duration` and
`duration / number` scale components (fractions cascade). Multiplying by an
integer is exact, and so is dividing by an integer that divides months, days
and the exact time part (`duration({hours: 10000}) / 2`). Other factors and
divisors scale in `f64`, which is approximate: an exact time part beyond 2^53
nanoseconds (about 104 days) loses nanosecond precision. `instant - instant`
is not supported (use `duration.between`). Scalar `+ - * / %` (with unary minus)
are now available in `WHERE` operands and `CASE`, not only in `RETURN`.

### Statement clock

All clock reads in one statement (`date()`, `datetime()`,
`CURRENT_TIMESTAMP()`, …) observe one instant: planning pins the clock with
`hawdb::temporal::with_stable_statement_clock`, and execution pins the same
instant again (also on parallel workers), so clock reads evaluated per row,
such as `datetime({timezone: n.tz})`, agree with clock reads bound during
planning. Hosts and tests may pin it explicitly with
`hawdb::temporal::with_statement_clock`. Statements that read the clock while
planning are non-deterministic and bypass the plan cache: zero-argument calls,
constructors whose map argument has only `timezone`, and statements whose
parameters include such a map (`datetime($tz)` with `$tz = {timezone: …}`).
`CURRENT_TIMESTAMP()`, `timestamp()`, `CAST(x AS TIMESTAMP)` and `date_part`
keep their integer epoch-nanosecond results.

## Planning and indexes

Temporal function calls are evaluated per row. In `WHERE`, a comparison
between a graph property and an expression whose inputs are all constants or
parameters is folded during planning, e.g.
`m.creationDate < datetime($start) + duration({days: $d})` becomes a native
`PropertyCompare` that storage filters, `CREATE RANGE INDEX` range seeks and
persistent range projections use. Such folded plans are cached per exact
parameter value: each distinct `$start` plans again and takes a new cache
entry. For a hot query, bind the temporal value itself as the parameter
(`m.creationDate < $end` with a `{"$datetime": …}` or native datetime
parameter); that plan is cached by parameter shape and rebound per execution. Range projections index dates, local times, local datetimes
and zoned datetimes (not durations). Zone maps never prune temporal values.

## Persistence and format compatibility

Every persistent encoder stores the same fixed-width little-endian payload
(`hawdb_core::temporal::codec`): date = `i32` days since 0001-01-01 (day 1);
local time = `u64` nanoseconds of the day; local datetime = date + time; zoned
datetime = UTC date + UTC time + `i32` offset seconds; duration = `i64` months,
`i64` days, `i64` seconds (floored) and `u32` nanoseconds. Decoding validates
every field and treats out-of-range data as corruption.

| Format | Encoding |
| --- | --- |
| WAL binary | value fields 11–15 (LEN payload) |
| Canonical checkpoint and persistent property projections | value tags 11–15 + payload |
| Text checkpoint, predicate checkpoint, source-scan reference | `D`, `T`, `L`, `Z`, `P` + canonical ISO text (non-canonical text is rejected) |
| Executor spill | tags 10–14 + payload |
| Columnar shadow | temporal columns stay in the residual property rows; no chunk encoding or zone-map bit is used |

No descriptor or columnar `FORMAT_VERSION` changed: value tags are additive
and self-describing, existing tags are unchanged (new binaries read old data),
and every decoder already rejects unknown tags — WAL readers skip unknown
fields and then refuse the empty value — so older binaries fail closed on the
first temporal value instead of misreading it. Persisted index statistics keys
encode temporals as `t<kind><hex payload>`.

## Bindings and JSON

FFI, Go, CLI, developer and compatibility JSON use single-key tagged ISO
strings, accepted as parameters and produced in results:
`{"$date": "2026-10-30"}`, `{"$localtime": "12:30:00"}`,
`{"$localdatetime": "2026-10-30T12:30:00"}`,
`{"$datetime": "2026-10-30T12:00:00+02:00"}`, `{"$duration": "P1M2DT3.5S"}`.
Go exposes them as the string types `Date`, `LocalTime`, `LocalDateTime`,
`DateTime` and `Duration`. Python returns the same tagged dicts (Python's
`datetime` has microsecond precision and `timedelta` has no months) and also
accepts `datetime.date`, naive `datetime.time`, naive and aware
`datetime.datetime` and `datetime.timedelta` parameters. The retained
zero-copy node APIs report temporal values as unsupported types. SQL keeps its
integer `DATE`/`TIMESTAMP` behaviour and refuses temporal parameters.

## Differences from Neo4j

* Fixed UTC offsets only; no named zones, no `ZONED TIME`.
* `datetime.transaction()/statement()/realtime()`, `*.truncate` with an
  override map, and the `P2012-02-02T14:37:21` duration form are not supported.
* Mixed-kind comparisons are `false`, not `NULL` (duration ordering is `NULL`).
* `epochMillis`/`epochSeconds` exist only on zoned datetimes; UNWIND mutations
  store temporal values from parameters (`row.created`), not `datetime(row.x)`.
* `RETURN` items still cannot be bare comparisons (`RETURN a < b`); use `CASE`.
