defmodule Sls.Writer do
  @moduledoc false
  use GenServer
  alias Sls.DataFile
  alias Sls.Index
  alias Sls.Record

  @tombstone "sls_tombstone"
  @header_size 14
  @crc_size 4
  @max_key_size 16_384
  @max_value_size 4_294_967_296

  def start_link(opts) do
    path = Keyword.fetch!(opts, :path)
    table = Keyword.fetch!(opts, :table)
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{path: path, table: table}, name: name)
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
  def init(%{path: path, table: table}) do
    datafile = DataFile.open!(%{id: 1, path: path, readonly?: false})
    offsets = load_offsets(datafile)

    last_offset =
      if map_size(offsets) > 0 do
        offsets
        |> Map.values()
        |> Enum.map(fn {offset, size, _value} -> offset + size end)
        |> Enum.max()
      else
        0
      end

    offsets
    |> Map.filter(fn {_k, {_offset, _size, value}} -> value != tombstone() end)
    |> Map.to_list()
    |> Map.new(fn {k, {offset, size, _value}} -> {k, {offset, size}} end)
    |> Index.init(table: table)

    {:ok, %{df: datafile, current_offset: last_offset, table: table}}
  end

  @impl true
  def handle_call(
        {:put, key, value},
        _from,
        %{df: datafile, current_offset: current_offset, table: table} = state
      ) do
    %{binary_payload: payload, record_size: record_size, crc: crc} =
      {key, value}
      |> Record.from_kv()
      |> Record.to_binary()

    case DataFile.append(datafile, crc <> payload) do
      {:ok, datafile} ->
        Index.insert(table, key, current_offset, record_size + @crc_size)

        {:reply, {:ok, {current_offset, record_size + @crc_size}},
         %{state | df: datafile, current_offset: current_offset + record_size + @crc_size}}

      _error ->
        {:reply, {:error, {current_offset, 0}}, state}
    end
  end

  @impl true
  def handle_call(
        {:delete, key},
        _from,
        %{df: datafile, current_offset: current_offset, table: table} = state
      ) do
    %{binary_payload: payload, record_size: record_size, crc: crc} =
      {key, tombstone()}
      |> Record.from_kv()
      |> Record.to_binary()

    case DataFile.append(datafile, crc <> payload) do
      {:ok, datafile} ->
        Index.delete(table, key)

        {:reply, {:ok, {current_offset, record_size + @crc_size}},
         %{state | df: datafile, current_offset: current_offset + record_size + @crc_size}}

      _error ->
        {:reply, {:error, {current_offset, 0}}, state}
    end
  end

  @impl true
  def terminate(reason, %{table: table, df: datafile}) do
    Index.shutdown(table)
    DataFile.close(datafile)
    IO.puts("Terminating: #{reason}")
  end

  defp load_offsets(datafile, offsets \\ %{}, current_offset \\ 0) do
    with {:ok, {data, datafile}} <-
           DataFile.read_at(datafile, current_offset, @header_size + @crc_size),
         {key_size, value_size} <- decode_header(data),
         {:ok, {key, datafile}} <-
           DataFile.read_at(datafile, current_offset + @header_size + @crc_size, key_size),
         {:ok, {value, datafile}} <-
           DataFile.read_at(
             datafile,
             current_offset + @header_size + @crc_size + key_size,
             value_size
           ) do
      value_offset = current_offset + @header_size + @crc_size + key_size + value_size

      offsets = Map.put(offsets, key, {current_offset, value_offset - current_offset, value})

      load_offsets(datafile, offsets, value_offset)
    else
      error ->
        case error do
          :eof -> offsets
          {:error, _reason} = e -> e
        end
    end
  end

  defp decode_header(data) do
    <<_crc::binary-size(4), _timestamp::binary-size(8), key_size::big-unsigned-integer-size(16),
      value_size::big-unsigned-integer-size(32)>> = data

    {key_size, value_size}
  end
end
