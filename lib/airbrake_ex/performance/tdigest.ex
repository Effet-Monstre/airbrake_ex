defmodule AirbrakeEx.Performance.TDigest do
  @moduledoc false
  # A merging t-digest: a list of `{mean, weight}` centroids, small at the tails and larger in
  # the middle, so percentiles stay accurate without keeping every timing. Encoded in Airbrake's
  # binary format (version 1: big-endian, the means as 64-bit floats then the weights as 32-bit
  # integers, base64).

  @compression 100

  @type centroid :: {float, non_neg_integer}

  @doc "Merges centroids (or raw values, weight 1) into at most about `compression` centroids."
  @spec compress([centroid]) :: [centroid]
  def compress([]), do: []

  def compress(centroids) do
    [first | rest] = Enum.sort_by(centroids, &elem(&1, 0))
    total = centroids |> Enum.map(&elem(&1, 1)) |> Enum.sum()

    {done, current, _seen} =
      Enum.reduce(rest, {[], first, 0}, fn {mean, weight}, {done, {cmean, cweight}, seen} ->
        merged = cweight + weight

        if k((seen + merged) / total) - k(seen / total) <= 1 do
          {done, {cmean + (mean - cmean) * weight / merged, merged}, seen}
        else
          {[{cmean, cweight} | done], {mean, weight}, seen + cweight}
        end
      end)

    Enum.reverse([current | done])
  end

  @doc "Adds raw values to centroids."
  @spec add([centroid], [number]) :: [centroid]
  def add(centroids, values), do: compress(centroids ++ Enum.map(values, &{&1 * 1.0, 1}))

  @doc "The digest as Airbrake's `tdigest` field."
  @spec encode([centroid]) :: String.t()
  def encode(centroids) do
    means = for {mean, _weight} <- centroids, into: <<>>, do: <<mean * 1.0::float-64>>
    weights = for {_mean, weight} <- centroids, into: <<>>, do: <<round(weight)::32>>

    Base.encode64(
      <<1::32, @compression * 1.0::float-64, length(centroids)::32, means::binary,
        weights::binary>>
    )
  end

  # The k1 scale function: a centroid spans at most one unit of k.
  defp k(q), do: @compression / (2 * :math.pi()) * :math.asin(2 * min(max(q, 0.0), 1.0) - 1)
end
