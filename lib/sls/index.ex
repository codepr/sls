defmodule Sls.Index do
  @moduledoc false

  @table :index_map

  def init(table \\ @table) do
    :ets.new(table, [:named_table, :protected, read_concurrency: true])
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

  def shutdown(table), do: :ets.delete(table)
  def shutdown, do: shutdown(@table)
end
