defmodule AirbrakeEx.PerformanceTest do
  use ExUnit.Case

  alias AirbrakeEx.Performance
  alias AirbrakeEx.Performance.TDigest

  @project_id "project_id"
  @project_key "project_key"

  setup do
    bypass = Bypass.open()
    Application.put_env(:airbrake_ex, :endpoint, "http://localhost:#{bypass.port}")
    Application.put_env(:airbrake_ex, :project_id, @project_id)
    Application.put_env(:airbrake_ex, :project_key, @project_key)
    Application.put_env(:airbrake_ex, :environment, "test")
    Application.put_env(:airbrake_ex, :performance_stats, true)

    on_exit(fn ->
      for key <- [:endpoint, :project_id, :project_key, :environment, :performance_stats],
          do: Application.delete_env(:airbrake_ex, key)
    end)

    test = self()

    Bypass.stub(bypass, "PUT", "/api/v5/projects/#{@project_id}/:kind", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)

      send(
        test,
        {:sent, conn.request_path |> String.split("/") |> List.last(), conn.req_headers,
         Jason.decode!(body)}
      )

      Plug.Conn.resp(conn, 204, "")
    end)

    {:ok, bypass: bypass}
  end

  defp start(opts \\ []) do
    start_supervised!(
      {Performance, Keyword.merge([repos: [[:test, :repo]], flush_interval: nil], opts)}
    )
  end

  defp ms(ms), do: System.convert_time_unit(ms * 1000, :microsecond, :native)

  defp received(kind) do
    assert_received {:sent, ^kind, headers, body}
    {headers, body}
  end

  defp decode(digest) do
    <<1::32, _compression::float-64, size::32, rest::binary>> = Base.decode64!(digest)
    <<means::binary-size(^size * 8), weights::binary-size(^size * 4)>> = rest
    Enum.zip(for(<<m::float-64 <- means>>, do: m), for(<<w::32 <- weights>>, do: w))
  end

  defp conn(method, status, type \\ "application/json; charset=utf-8"),
    do: %Plug.Conn{method: method, status: status, resp_headers: [{"content-type", type}]}

  defp request(route, duration, queries) do
    conn = conn("GET", 200)
    :telemetry.execute([:phoenix, :router_dispatch, :start], %{}, %{conn: conn, route: route})

    for {sql, time} <- queries,
        do: :telemetry.execute([:test, :repo, :query], %{total_time: ms(time)}, %{query: sql})

    :telemetry.execute([:phoenix, :router_dispatch, :stop], %{duration: ms(duration)}, %{
      conn: conn,
      route: route
    })
  end

  test "doesn't start unless turned on" do
    Application.put_env(:airbrake_ex, :performance_stats, false)
    assert :ignore = Performance.start_link(flush_interval: nil)
    assert Performance.measure("RPC", "x", fn -> :done end) == :done
  end

  test "sends routes by pattern, with their database time, and the queries they ran" do
    start()
    request("/users/:id", 30, [{"SELECT 1", 4}, {"SELECT 2", 6}])
    request("/users/:id", 10, [])

    Performance.flush()

    {headers, body} = received("routes-stats")
    assert {"authorization", "Bearer #{@project_key}"} in headers
    assert body["environment"] == "test"

    assert [route] = body["routes"]
    assert %{"method" => "GET", "route" => "/users/:id", "statusCode" => 200} = route
    assert route["count"] == 2
    assert_in_delta route["sum"], 40, 0.01
    assert_in_delta route["sumsq"], 30 * 30 + 10 * 10, 0.1
    assert {:ok, _time, 0} = DateTime.from_iso8601(route["time"])
    assert route["tdigest"] |> decode() |> Enum.map(&elem(&1, 1)) |> Enum.sum() == 2

    {_headers, body} = received("routes-breakdowns")
    assert [breakdown] = body["routes"]
    assert %{"route" => "/users/:id", "responseType" => "json", "count" => 1} = breakdown
    assert %{"db" => %{"count" => 1, "sum" => db}} = breakdown["groups"]
    assert_in_delta db, 10, 0.01

    {_headers, body} = received("queries-stats")
    queries = Enum.sort_by(body["queries"], & &1["query"])

    assert [%{"query" => "SELECT 1", "route" => "GET /users/:id"}, %{"query" => "SELECT 2"}] =
             queries

    refute_received {:sent, "queues-stats", _, _}
  end

  test "counts a raising route as a 500" do
    start()
    meta = %{conn: conn("POST", nil), route: "/boom", kind: :error, reason: :oops}
    :telemetry.execute([:phoenix, :router_dispatch, :start], %{}, meta)
    :telemetry.execute([:phoenix, :router_dispatch, :exception], %{duration: ms(5)}, meta)

    Performance.flush()

    {_headers, body} = received("routes-stats")
    assert [%{"method" => "POST", "route" => "/boom", "statusCode" => 500}] = body["routes"]
  end

  test "sends jobs as queues, with their errors" do
    start()
    job = %{worker: "MyApp.Worker", queue: "default"}

    for stage <- [:stop, :exception] do
      :telemetry.execute([:oban, :job, :start], %{}, %{job: job})
      :telemetry.execute([:test, :repo, :query], %{total_time: ms(2)}, %{query: "UPDATE jobs"})
      :telemetry.execute([:oban, :job, stage], %{duration: ms(20)}, %{job: job})
    end

    Performance.flush()

    {_headers, body} = received("queues-stats")

    assert [%{"queue" => "MyApp.Worker", "count" => 2, "errorCount" => 1} = queue] =
             body["queues"]

    assert %{"db" => %{"count" => 2, "sum" => db}} = queue["groups"]
    assert_in_delta db, 4, 0.01

    {_headers, body} = received("queries-stats")
    assert [%{"query" => "UPDATE jobs", "route" => "JOB MyApp.Worker"}] = body["queries"]
  end

  test "measures anything as a route, and a raise as a 500" do
    start()
    assert Performance.measure("STORE", "leads", fn -> :rows end) == :rows

    assert_raise RuntimeError, fn ->
      Performance.measure("STORE", "save_lead", fn -> raise "refused" end)
    end

    Performance.flush()

    {_headers, body} = received("routes-stats")

    assert [
             %{"method" => "STORE", "route" => "leads", "statusCode" => 200},
             %{"method" => "STORE", "route" => "save_lead", "statusCode" => 500}
           ] = Enum.sort_by(body["routes"], & &1["route"])
  end

  test "measures the application's spans as routes, named as they started, with their queries" do
    start(
      spans: [
        {[:app, :rpc], "RPC"},
        {[:app, :store], &(&1.kind |> to_string() |> String.upcase())}
      ]
    )

    :telemetry.span([:app, :rpc], %{name: "create_user"}, fn ->
      :telemetry.execute([:test, :repo, :query], %{total_time: ms(3)}, %{query: "INSERT users"})
      {:ok, %{}}
    end)

    :telemetry.span([:app, :store], %{kind: :mutation, name: :save}, fn -> {:ok, %{}} end)

    assert_raise RuntimeError, fn ->
      :telemetry.span([:app, :rpc], %{name: "boom"}, fn -> raise "refused" end)
    end

    Performance.flush()

    {_headers, body} = received("routes-stats")

    assert [
             %{"method" => "MUTATION", "route" => "save", "statusCode" => 200},
             %{"method" => "RPC", "route" => "boom", "statusCode" => 500},
             %{"method" => "RPC", "route" => "create_user", "statusCode" => 200}
           ] = Enum.sort_by(body["routes"], &{&1["method"], &1["route"]})

    {_headers, body} = received("routes-breakdowns")
    assert [%{"route" => "create_user", "groups" => %{"db" => %{"sum" => db}}}] = body["routes"]
    assert_in_delta db, 3, 0.01

    {_headers, body} = received("queries-stats")
    assert [%{"query" => "INSERT users", "route" => "RPC create_user"}] = body["queries"]
  end

  test "refuses a span it can't name" do
    assert_raise ArgumentError, fn ->
      Performance.init(enabled: true, flush_interval: nil, spans: [{[:app, :rpc], :rpc}])
    end
  end

  test "names LiveView events and channel messages by module" do
    start()
    socket = %{view: MyApp.PageLive, channel: MyApp.RoomChannel}

    :telemetry.execute([:phoenix, :live_view, :handle_event, :start], %{}, %{
      socket: socket,
      event: "save"
    })

    :telemetry.execute([:phoenix, :live_view, :handle_event, :stop], %{duration: ms(3)}, %{
      socket: socket,
      event: "save"
    })

    :telemetry.execute([:phoenix, :channel_handled_in], %{duration: ms(1)}, %{
      socket: socket,
      event: "ping"
    })

    Performance.flush()

    {_headers, body} = received("routes-stats")

    assert [
             %{"method" => "CHANNEL", "route" => "MyApp.RoomChannel#ping"},
             %{"method" => "LIVE", "route" => "MyApp.PageLive#save"}
           ] = Enum.sort_by(body["routes"], & &1["method"])
  end

  test "sends nothing when nothing was measured" do
    start()
    assert Performance.flush() == []
    refute_received {:sent, _, _, _}
  end

  test "sends what's left when it stops" do
    start()
    Performance.measure("RPC", "last", fn -> :ok end)
    stop_supervised!(Performance)

    {_headers, body} = received("routes-stats")
    assert [%{"route" => "last"}] = body["routes"]
  end

  describe "TDigest" do
    test "keeps every value's weight in few centroids, and the tails sharp" do
      values = for i <- 1..10_000, do: i * 1.0
      digest = values |> Enum.chunk_every(256) |> Enum.reduce([], &TDigest.add(&2, &1))

      assert length(digest) < 120
      assert digest |> Enum.map(&elem(&1, 1)) |> Enum.sum() == 10_000
      assert digest |> List.first() |> elem(0) < 5
      assert digest |> List.last() |> elem(0) > 9_995

      assert decode(TDigest.encode(digest)) == Enum.map(digest, fn {m, w} -> {m, w} end)
    end
  end
end
