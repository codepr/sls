defmodule Sls.Writer do
  @moduledoc false
  use GenServer
  alias Sls.Index
  alias Sls.Record

  @tombstone "sls_tombstone"
  @header_size 14
  @crc_size 4
  @max_key_size 16_384
  @max_value_size 4_294_967_296

  def start_link(opts) do
    log_path = Keyword.fetch!(opts, :log_path)
    table = Keyword.fetch!(opts, :table)
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{log_path: log_path, table: table}, name: name)
  end

  def put(key, value), do: put(__MODULE__, key, value)

  def put(pid, key, value) do
    cond do
      IO.iodata_length(key) > @max_key_size ->
        {:error, :invalid_key_size}

      IO.iodata_length(value) > @max_value_size ->
        {:error, :invalid_value_size}

      value == tombstone() ->
        {:error, :invalid_value_tombstone}

      true ->
        GenServer.call(pid, {:put, key, value})
    end
  end

  def delete(key), do: delete(__MODULE__, key)

  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  def tombstone, do: @tombstone

  @impl true
  def init(%{log_path: log_path, table: table}) do
    fd = File.open!(log_path, [:write, :read, :binary])
    offsets = load_offsets(fd)
    Index.init(offsets, table: table)
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
  def terminate(reason, %{table: table, fd: fd}) do
    Index.shutdown(table)
    File.close(fd)
    IO.puts("Terminating: #{reason}")
  end

  defp load_offsets(fd, offsets \\ %{}, current_offset \\ 0) do
    :file.position(fd, current_offset)

    with <<_timestamp::big-unsigned-integer-size(64), key_size::big-unsigned-integer-size(16),
           value_size::big-unsigned-integer-size(32)>> <- IO.binread(fd, @header_size),
         key <- IO.binread(fd, key_size) do
      value_obs_offset = current_offset + @header_size + key_size
      offsets = Map.put(offsets, key, {value_obs_offset, value_size})
      load_offsets(fd, offsets, value_obs_offset + value_size)
    else
      :eof -> offsets
    end
  end
end
