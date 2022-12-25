defmodule Sls.Writer do
  @moduledoc false
  use GenServer
  alias Sls.Index
  alias Sls.Record

  @tombstone "sls_tombstone"
  @crc_size 4

  def start_link(opts) do
    log_path = Keyword.fetch!(opts, :log_path)
    table = Keyword.fetch!(opts, :table)
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{log_path: log_path, table: table}, name: name)
  end

  def put(key, value), do: put(__MODULE__, key, value)

  def put(_pid, _key, @tombstone), do: raise("Tombstone value not valid as value")

  def put(pid, key, value) do
    GenServer.call(pid, {:put, key, value})
  end

  def delete(key), do: delete(__MODULE__, key)

  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  def tombstone, do: @tombstone

  @impl true
  def init(%{log_path: log_path, table: table}) do
    fd = File.open!(log_path, [:write, :binary])
    Index.init(log_path: log_path, table: table)
    {:ok, %{fd: fd, current_offset: 0, table: table}}
  end

  @impl true
  def handle_call(
        {:put, key, value},
        _from,
        %{fd: fd, current_offset: current_offset, table: table} = state
      ) do
    %{binary_payload: payload, record_size: record_size, crc: crc} =
      {key, value}
      |> Record.from_kv()
      |> Record.to_binary()

    :ok = IO.binwrite(fd, crc <> payload)
    Index.insert(table, key, current_offset, record_size + @crc_size)

    {:reply, {:ok, {current_offset, record_size + @crc_size}},
     %{state | current_offset: current_offset + record_size + @crc_size}}
  end

  @impl true
  def handle_call(
        {:delete, key},
        _from,
        %{fd: fd, current_offset: current_offset, table: table} = state
      ) do
    %{binary_payload: payload, record_size: record_size, crc: crc} =
      {key, tombstone()}
      |> Record.from_kv()
      |> Record.to_binary()

    :ok = IO.binwrite(fd, crc <> payload)
    Index.delete(table, key)

    {:reply, {:ok, {current_offset, record_size + @crc_size}},
     %{state | current_offset: current_offset + record_size + @crc_size}}
  end

  @impl true
  def terminate(reason, %{table: table}) do
    Index.shutdown(table)
    IO.puts("Terminating: #{reason}")
  end
end
