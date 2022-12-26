defmodule Sls.Reader do
  @moduledoc false
  use GenServer
  alias Sls.DataFile
  alias Sls.Index
  alias Sls.Record

  def start_link(opts) do
    log_path = Keyword.fetch!(opts, :log_path)
    table = Keyword.fetch!(opts, :table)
    single_process = Keyword.get(opts, :single_process, false)

    GenServer.start_link(__MODULE__, %{
      log_path: log_path,
      table: table,
      single_process: single_process,
      datafiles: []
    })
  end

  def get(key), do: get(__MODULE__, key)

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  def add_new_datafile(pid, log_path) do
    GenServer.call(pid, {:add_new_datafile, log_path})
  end

  @impl true
  def init(%{log_path: log_path, table: table, single_process: single_process}) do
    datafile = DataFile.open!(%{id: 1, path: log_path, readonly?: true})
    if !single_process, do: Registry.register(Sls.ReaderPool, :readers, _value = nil)
    {:ok, %{df: datafile, table: table}}
  end

  @impl true
  def handle_call({:get, key}, _from, %{df: datafile, table: table} = state) do
    with {:ok, {offset, size}} <- Index.lookup(table, key),
         {:ok, {data, datafile}} <- DataFile.read_at(datafile, offset, size),
         <<crc::binary-size(4), rest::binary>> = data,
         true <- is_valid?(rest, crc),
         record <- Record.from_binary(rest) do
      {:reply, {:ok, record.value}, %{state | df: datafile}}
    else
      false ->
        {:reply, {:error, :corrupted_data}, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:add_new_datafile, log_path}, _from, %{datafiles: datafiles} = state) do
    datafile = DataFile.open!(%{id: 1, path: log_path, readonly?: true})
    {:reply, :ok, %{state | datafiles: [datafile | datafiles]}}
  end

  defp is_valid?(data, crc) do
    decoded_crc = :binary.decode_unsigned(crc)
    :erlang.crc32(data) == decoded_crc
  end
end
