defmodule Sls.Index do
  @moduledoc false

  @table Application.compile_env!(:sls, :default_cache_table)

  def init(offsets, opts \\ []) do
    table = Keyword.get(opts, :table, @table)
    warm_up(table, offsets)
  end

  def insert(table, key, offset, size) do
    :ets.insert(table, {key, {offset, size}})
    :ok
  end

  def insert(key, offset, size), do: insert(@table, key, offset, size)

  def lookup(table, key) do
    case :ets.lookup(table, key) do
      [{^key, {offset, size}}] -> {:ok, {offset, size}}
      [] -> {:error, :not_found}
    end
  end

  def lookup(key), do: lookup(@table, key)

  def delete(table, key) do
    :ets.delete(table, key)
    :ok
  end

  def shutdown(table), do: :ets.delete(table)
  def shutdown, do: shutdown(@table)

  defp warm_up(table, offsets) do
    table
    |> :ets.new([:named_table, :protected, read_concurrency: true])
    |> :ets.insert(Map.to_list(offsets))
  end
end
