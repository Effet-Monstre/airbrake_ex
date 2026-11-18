# Changelog

## 0.3.0 (Effet Monstre fork)
- `AirbrakeEx.Performance`: sends performance stats (Airbrake's v5 `routes-stats`,
  `routes-breakdowns`, `queries-stats`, `queues-stats`) from the Phoenix, LiveView, channel,
  Ecto and Oban telemetry events, about once a minute; `AirbrakeEx.Performance.measure/4` for
  anything else. Off unless `performance_stats: true` and started in the supervision tree.
- `AirbrakeEx.Plug` reports the exception a `Plug.Conn.WrapperError` wraps.

## 0.2.9
- Fix a bug introduced in 0.2.8 with the decoding of the exception

## 0.2.8
- Hides filter_parameters option in the exception message and stack trace


## 0.2.7

- Hides all parameters containing a password
- Enhance documentation of filter_parameters option
