defmodule AirbrakeEx.Performance do
  @moduledoc """
  Sends performance stats to Airbrake (its "Performance Monitoring" API, v5): how long routes,
  database queries and background jobs take, about once a minute.

  It listens to the `:telemetry` events the usual libraries already emit, keeps per-minute
  aggregates in memory (count, sum, sum of squares and a t-digest for the percentiles) and puts
  them to `routes-stats`, `routes-breakdowns`, `queries-stats` and `queues-stats`:

    * routes: `[:phoenix, :router_dispatch, ...]` (named by the route pattern, `/users/:id`,
      never the request's path), LiveView mounts, `handle_params` and events, channel joins and
      events (`channels: false` turns those off);
    * the time a route spent in the database, as its breakdown's `db` group;
    * queries: each repo's `[..., :query]` event, named by its SQL (parameterized: no values);
    * queues: Oban jobs (`[:oban, :job, ...]`), named by worker, errors counted;
    * the application's own `:telemetry.span/3`s, given in `:spans`, as routes.

  Anything else is measured with `measure/4`.

  ## Setup

  Turn it on in the configuration, next to the project's id and key:

      config :airbrake_ex,
        project_id: 123456,
        project_key: "abcdef12345",
        performance_stats: true

  Then start it in the application's supervision tree, after the repos:

      children = [
        MyApp.Repo,
        {AirbrakeEx.Performance, repos: [MyApp.Repo]},
        MyAppWeb.Endpoint
      ]

  Without `performance_stats: true` (or a project id and key) it doesn't start (`:ignore`) and
  attaches to nothing.

  ## Options

    * `:repos` - the Ecto repos whose queries are measured, as modules (their
      `:telemetry_prefix`) or telemetry prefixes (`[:my_app, :repo]`). Default: none.
    * `:channels` - measure Phoenix channel joins and events. Default: `true`.
    * `:spans` - the application's own spans (`:telemetry.span/3`), measured as routes:
      `{event_prefix, method}`, named by the start metadata's `:name`; `method` is a string or
      a function of that metadata. The queries run inside are attributed to the span, an
      exception counts as a 500. Default: none.

          spans: [
            {[:my_app, :rpc], "RPC"},
            {[:my_app, :store, :run], &(&1.kind |> to_string() |> String.upcase())}
          ]
    * `:flush_interval` - milliseconds between two sends. Default: one minute.
    * `:enabled` - overrides `performance_stats`.
    * `:name` - the server's name. Default: `AirbrakeEx.Performance`.
  """

  use GenServer
  require Logger

  alias AirbrakeEx.Config
  alias AirbrakeEx.Performance.TDigest

  @flush_interval :timer.minutes(1)
  # Values kept raw before they're folded into the digest.
  @pending 256
  # Distinct routes, queries and jobs per minute; past it a new one is dropped until the next send.
  @max_keys 5000
  # Rows per request.
  @batch 1000
  @max_name 4000

  @current {__MODULE__, :current}
  @db {__MODULE__, :db}

  @destinations [
    route: {"routes-stats", "routes"},
    breakdown: {"routes-breakdowns", "routes"},
    query: {"queries-stats", "queries"},
    queue: {"queues-stats", "queues"}
  ]

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      shutdown: 10_000
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Measures `fun` as a route named `method` and `route` (for example `"STORE"` and a query's
  name), with the database time spent in it, and returns its result. A raise or a throw is
  measured with the status 500 and goes on.

  Queries run inside are attributed to it (their `route`).

  ## Options

    * `:status` - the status of a call that returns. Default: 200.
    * `:response_type` - its breakdown's type. Default: `"other"`.
    * `:server` - the server's name. Default: `AirbrakeEx.Performance`.
  """
  def measure(method, route, fun, opts \\ []) when is_function(fun, 0) do
    server = Keyword.get(opts, :server, __MODULE__)

    if Process.whereis(server) do
      start = System.monotonic_time()
      enter(method, route)

      try do
        result = fun.()
        stop(server, method, route, Keyword.get(opts, :status, 200), opts, start)
        result
      catch
        kind, reason ->
          stop(server, method, route, 500, opts, start)
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
    else
      fun.()
    end
  end

  defp stop(server, method, route, status, opts, start) do
    db = leave()
    duration = System.monotonic_time() - start
    type = Keyword.get(opts, :response_type, "other")
    route_stat(server, method, route, status, type, duration, db)
  end

  @doc "Sends what's been measured so far, now. Returns the requests' results."
  def flush(server \\ __MODULE__), do: GenServer.call(server, :flush, 30_000)

  ## Server

  @impl true
  def init(opts) do
    spans = Map.new(Keyword.get(opts, :spans, []), &valid_span/1)

    if enabled?(opts) do
      Process.flag(:trap_exit, true)
      name = Keyword.get(opts, :name, __MODULE__)
      handler = {__MODULE__, name}
      events = events(opts)
      :telemetry.detach(handler)
      config = %{server: name, spans: spans}
      :ok = :telemetry.attach_many(handler, events, &__MODULE__.handle_event/4, config)

      interval = Keyword.get(opts, :flush_interval, @flush_interval)
      if interval, do: Process.send_after(self(), :flush, interval)

      {:ok, %{handler: handler, interval: interval, stats: %{}, since: now()}}
    else
      :ignore
    end
  end

  defp enabled?(opts) do
    Keyword.get_lazy(opts, :enabled, fn -> Config.get(:airbrake_ex, :performance_stats, false) end) &&
      Config.get(:airbrake_ex, :project_id) not in [nil, ""] &&
      Config.get(:airbrake_ex, :project_key) not in [nil, ""]
  end

  defp events(opts) do
    phoenix =
      for stage <- [:start, :stop, :exception], do: [:phoenix, :router_dispatch, stage]

    live =
      for callback <- [:mount, :handle_params, :handle_event],
          stage <- [:start, :stop, :exception],
          do: [:phoenix, :live_view, callback, stage]

    channels =
      if Keyword.get(opts, :channels, true),
        do: [[:phoenix, :channel_joined], [:phoenix, :channel_handled_in]],
        else: []

    oban = for stage <- [:start, :stop, :exception], do: [:oban, :job, stage]

    repos = for repo <- Keyword.get(opts, :repos, []), do: prefix(repo) ++ [:query]

    spans =
      for {prefix, _method} <- Keyword.get(opts, :spans, []),
          stage <- [:start, :stop, :exception],
          do: prefix ++ [stage]

    phoenix ++ live ++ channels ++ oban ++ repos ++ spans
  end

  defp valid_span({prefix, method} = span)
       when is_list(prefix) and (is_binary(method) or is_function(method, 1)),
       do: span

  defp valid_span(span),
    do: raise(ArgumentError, "expected a span as {event_prefix, method}, got: #{inspect(span)}")

  defp prefix(prefix) when is_list(prefix), do: prefix

  defp prefix(repo) when is_atom(repo) do
    repo.config()[:telemetry_prefix] ||
      repo |> Module.split() |> Enum.map(&(&1 |> Macro.underscore() |> String.to_atom()))
  end

  @impl true
  def handle_cast({:add, key, ms, extra}, state) do
    case state.stats do
      %{^key => stat} ->
        {:noreply, put_in(state.stats[key], add(stat, ms, extra))}

      stats when map_size(stats) < @max_keys ->
        {:noreply, put_in(state.stats[key], add(new_stat(), ms, extra))}

      _stats ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_call(:flush, _from, state) do
    {stats, state} = take(state)
    {:reply, send_stats(stats), state}
  end

  @impl true
  def handle_info(:flush, state) do
    Process.send_after(self(), :flush, state.interval)
    {stats, state} = take(state)
    if stats != [], do: Task.start(fn -> send_stats(stats) end)
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :telemetry.detach(state.handler)
    {stats, _state} = take(state)
    send_stats(stats)
    :ok
  end

  defp take(state), do: {rows(state.stats, state.since), %{state | stats: %{}, since: now()}}

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second) |> Map.put(:second, 0)

  ## Telemetry

  @doc false
  def handle_event([:phoenix, :router_dispatch, :start], _measurements, meta, _config),
    do: enter(meta.conn.method, meta[:route] || "unknown")

  def handle_event([:phoenix, :router_dispatch, stage], %{duration: duration}, meta, config) do
    db = leave()
    conn = meta.conn
    status = if stage == :exception, do: 500, else: conn.status
    route = meta[:route] || "unknown"
    route_stat(config.server, conn.method, route, status, response_type(conn), duration, db)
  end

  def handle_event([:phoenix, :live_view, callback, :start], _measurements, meta, _config),
    do: enter("LIVE", live_route(meta, callback))

  def handle_event([:phoenix, :live_view, callback, stage], %{duration: d}, meta, config) do
    db = leave()
    status = if stage == :exception, do: 500, else: 200
    route_stat(config.server, "LIVE", live_route(meta, callback), status, "live", d, db)
  end

  def handle_event([:phoenix, :channel_joined], %{duration: duration}, meta, config) do
    status = if match?({:ok, _}, meta[:result]) or meta[:result] == :ok, do: 200, else: 403

    route_stat(
      config.server,
      "CHANNEL",
      "#{inspect(meta.socket.channel)}#join",
      status,
      "channel",
      duration,
      nil
    )
  end

  def handle_event([:phoenix, :channel_handled_in], %{duration: duration}, meta, config) do
    route = "#{inspect(meta.socket.channel)}##{meta.event}"
    route_stat(config.server, "CHANNEL", route, 200, "channel", duration, nil)
  end

  def handle_event([:oban, :job, :start], _measurements, meta, _config),
    do: enter("JOB", meta.job.worker)

  def handle_event([:oban, :job, stage], %{duration: duration}, meta, config) do
    db = leave()
    groups = if db && db > 0, do: %{"db" => db}, else: %{}
    error = if stage == :exception, do: 1, else: 0
    cast(config.server, {:queue, meta.job.worker}, ms(duration), %{error: error, groups: groups})
  end

  def handle_event(event, measurements, meta, config) do
    {prefix, [stage]} = Enum.split(event, -1)

    case Map.fetch(config.spans, prefix) do
      {:ok, method} -> span(stage, method, measurements, meta, config)
      :error -> if stage == :query, do: query(measurements, meta, config)
    end
  end

  defp span(:start, method, _measurements, meta, _config) do
    method = if is_function(method), do: method.(meta), else: method
    enter(to_string(method), to_string(meta[:name] || "unknown"))
  end

  # Named as it started: a span's stop metadata is what its function returned.
  defp span(stage, _method, %{duration: duration}, _meta, config) do
    case pop() do
      {method, route, db} ->
        status = if stage == :exception, do: 500, else: 200
        route_stat(config.server, method, route, status, "other", duration, db)

      nil ->
        :ok
    end
  end

  defp query(measurements, %{query: sql}, config) when is_binary(sql) do
    time =
      measurements[:total_time] ||
        Enum.sum(for key <- [:queue_time, :query_time, :decode_time], do: measurements[key] || 0)

    ms = ms(time)
    Process.put(@db, Process.get(@db, 0.0) + ms)

    route =
      case Process.get(@current) do
        [{method, route, _db} | _] -> "#{method} #{route}"
        _ -> nil
      end

    cast(config.server, {:query, String.slice(sql, 0, @max_name), route}, ms, %{})
  end

  defp query(_measurements, _meta, _config), do: :ok

  defp live_route(%{socket: socket, event: event}, :handle_event),
    do: "#{inspect(socket.view)}##{event}"

  defp live_route(%{socket: socket}, callback), do: "#{inspect(socket.view)}##{callback}"

  defp response_type(%{resp_headers: headers}) do
    case List.keyfind(headers, "content-type", 0) do
      {_, "application/json" <> _} -> "json"
      {_, "text/html" <> _} -> "html"
      _ -> "other"
    end
  end

  defp response_type(_conn), do: "other"

  # What runs between `enter` and `leave` is the route's: its queries name it and add up as its
  # database time. A stack, for a route inside another (a forwarded router, `measure/4` in a job).
  defp enter(method, route) do
    Process.put(@current, [{method, route, Process.get(@db, 0.0)} | Process.get(@current, [])])
    :ok
  end

  # The database time since the matching `enter`, nil without one.
  defp leave do
    case pop() do
      {_method, _route, db} -> db
      nil -> nil
    end
  end

  # The innermost route, with its database time so far.
  defp pop do
    case Process.get(@current, []) do
      [{method, route, start} | rest] ->
        if rest == [], do: Process.delete(@current), else: Process.put(@current, rest)
        {method, route, Process.get(@db, 0.0) - start}

      [] ->
        nil
    end
  end

  defp route_stat(server, method, route, status, type, duration, db) do
    route = String.slice(to_string(route), 0, @max_name)
    ms = ms(duration)
    cast(server, {:route, method, route, status}, ms, %{})

    if db && db > 0,
      do: cast(server, {:breakdown, method, route, type}, ms, %{groups: %{"db" => db}})
  end

  defp cast(server, key, ms, extra) do
    if Process.whereis(server), do: GenServer.cast(server, {:add, key, ms, extra})
    :ok
  end

  defp ms(native), do: System.convert_time_unit(native, :native, :microsecond) / 1000

  ## Aggregates

  defp new_stat,
    do: %{count: 0, sum: 0.0, sumsq: 0.0, errors: 0, groups: %{}, digest: [], pending: []}

  defp add(stat, ms, extra) do
    stat = %{
      stat
      | count: stat.count + 1,
        sum: stat.sum + ms,
        sumsq: stat.sumsq + ms * ms,
        errors: stat.errors + Map.get(extra, :error, 0),
        groups: add_groups(stat.groups, Map.get(extra, :groups, %{})),
        pending: [ms | stat.pending]
    }

    if length(stat.pending) >= @pending, do: fold(stat), else: stat
  end

  defp fold(stat), do: %{stat | digest: TDigest.add(stat.digest, stat.pending), pending: []}

  defp add_groups(groups, new) do
    Enum.reduce(new, groups, fn {name, ms}, groups ->
      Map.update(groups, name, %{count: 1, sum: ms}, &%{count: &1.count + 1, sum: &1.sum + ms})
    end)
  end

  defp rows(stats, since) do
    time = DateTime.to_iso8601(since)

    for {key, stat} <- stats do
      stat = fold(stat)

      common = %{
        time: time,
        count: stat.count,
        sum: stat.sum,
        sumsq: stat.sumsq,
        tdigest: TDigest.encode(stat.digest)
      }

      row(key, stat, common)
    end
  end

  defp row({:route, method, route, status}, _stat, common),
    do: {:route, Map.merge(common, %{method: method, route: route, statusCode: status})}

  defp row({:breakdown, method, route, type}, stat, common) do
    fields = %{method: method, route: route, responseType: type, groups: stat.groups}
    {:breakdown, Map.merge(common, fields)}
  end

  defp row({:query, sql, route}, _stat, common) do
    fields = if route, do: %{query: sql, route: route}, else: %{query: sql}
    {:query, Map.merge(common, fields)}
  end

  defp row({:queue, worker}, stat, common) do
    fields = %{queue: worker, errorCount: stat.errors, groups: stat.groups}
    {:queue, Map.merge(common, fields)}
  end

  ## Sending

  defp send_stats([]), do: []

  defp send_stats(rows) do
    environment = to_string(Config.get(:airbrake_ex, :environment, "production"))
    grouped = Enum.group_by(rows, &elem(&1, 0), &elem(&1, 1))

    for {kind, {path, cargo}} <- @destinations,
        batch <- Enum.chunk_every(Map.get(grouped, kind, []), @batch) do
      body = Jason.encode!(%{"environment" => environment, cargo => batch})
      put(path, body)
    end
  end

  defp put(path, body) do
    project_id = Config.get(:airbrake_ex, :project_id)
    key = Config.get(:airbrake_ex, :project_key)
    endpoint = Config.get(:airbrake_ex, :endpoint, "https://api.airbrake.io")
    url = "#{endpoint}/api/v5/projects/#{project_id}/#{path}"
    headers = [{"Content-Type", "application/json"}, {"Authorization", "Bearer #{key}"}]

    case HTTPoison.put(url, body, headers, Config.get(:airbrake_ex, :http_options) || []) do
      {:ok, %{status_code: status}} when status in 200..299 ->
        {path, :ok}

      {:ok, %{status_code: status}} ->
        Logger.warning("AirbrakeEx.Performance: #{path} answered #{status}")
        {path, {:error, status}}

      {:error, error} ->
        Logger.warning("AirbrakeEx.Performance: #{path} failed: #{inspect(error)}")
        {path, {:error, error}}
    end
  end
end
