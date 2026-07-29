defmodule AirbrakeEx.Plug do
  @moduledoc """
  Add the `AirbrakeEx.Plug` to you web application Plug stack
  to send all exceptions to `airbrake`

  ```elixir
  defmodule YourApp.Router do
    use Phoenix.Router
    use AirbrakeEx.Plug

    # ...
  end
  ```
  """

  @doc false
  defmacro __using__(_env) do
    quote location: :keep do
      @before_compile AirbrakeEx.Plug
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote location: :keep do
      defoverridable call: 2

      def call(conn, opts) do
        try do
          super(conn, opts)
        rescue
          e in Plug.Conn.WrapperError ->
            %{conn: conn, kind: kind, reason: reason, stack: stack} = e
            AirbrakeEx.Plug.__catch__(conn, kind, reason, stack)
            reraise e, __STACKTRACE__

          e ->
            AirbrakeEx.Plug.__catch__(conn, :error, e, __STACKTRACE__)
            reraise e, __STACKTRACE__
        end
      end
    end
  end

  @doc false
  def __catch__(conn, kind, reason, stack) do
    reason = Exception.normalize(kind, reason, stack)
    parsed_exception = AirbrakeEx.ExceptionParser.parse(reason, stack)

    session =
      Map.get(conn.private, :plug_session)
      |> json_escape()

    AirbrakeEx.Notifier.notify(
      parsed_exception,
      params: conn.params,
      session: session,
      context: %{url: Plug.Conn.request_url(conn)}
    )
  end

  defp json_escape(v) when is_binary(v),
    do: if(String.valid?(v), do: v, else: inspect(v))

  defp json_escape(%{} = v),
    do: Map.new(v, fn {k, v} -> {k, json_escape(v)} end)

  defp json_escape(v) when is_list(v),
    do: Enum.map(v, &json_escape/1)

  defp json_escape(v),
    do: v
end
