defmodule Sls.Index do
  @moduledoc false
  use GenServer

  def start_link([]) do
    GenServer.start_link(__MODULE__, :empty, name: __MODULE__)
  end

  def init(:empty), do: {:ok, %{}}

  def upsert(key, offset, size) do
    GenServer.call(__MODULE__, {:upsert, key, offset, size})
  end

  def lookup(key) do
    GenServer.call(__MODULE__, {:lookup, key})
  end

  def handle_call({:upsert, key, offset, size}, _from, index_map) do
    {:reply, :ok, Map.put(index_map, key, {offset, size})}
  end

  def handle_call({:lookup, key}, _from, index_map) do
    offse_size =
      case Map.get(index_map, key) do
        nil -> {:error, :not_found}
        value -> {:ok, value}
      end

    {:reply, offse_size, index_map}
  end
end
