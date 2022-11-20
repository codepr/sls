defmodule Sls.Index do
  @moduledoc false

  def init() do
    :ets.new(__MODULE__, [:named_table, read_concurrency: true])
  end

  def insert(key, offset, size) do
    :ets.insert(__MODULE__, {key, {offset, size}})
    :ok
  end

  def lookup(key) do
    case :ets.lookup(__MODULE__, key) do
      [{^key, {offset, size}}] -> {:ok, {offset, size}}
      [] -> {:error, :not_found}
    end
  end
end
