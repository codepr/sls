defmodule Sls.Reader do
  @moduledoc false
  use GenServer
  alias Sls.Index
  alias Sls.Record

  def start_link(opts) do
    log_path = Keyword.fetch!(opts, :log_path)
    table = Keyword.fetch!(opts, :table)
    single_process = Keyword.get(opts, :single_process, false)

    GenServer.start_link(__MODULE__, %{
      log_path: log_path,
      table: table,
      single_process: single_process
    })
  end

  def get(key), do: get(__MODULE__, key)

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  @impl true
  def init(%{log_path: log_path, table: table, single_process: single_process}) do
    fd = File.open!(log_path, [:read, :binary])
    if !single_process, do: Registry.register(Sls.ReaderPool, :readers, _value = nil)
    {:ok, %{fd: fd, table: table}}
  end

  @impl true
  def handle_call({:get, key}, _from, %{fd: fd, table: table}) do
    with {:ok, {offset, size}} <- Index.lookup(table, key),
         {:ok, data} <- :file.pread(fd, offset, size),
         <<crc::binary-size(4), rest::binary>> = data,
         true <- is_valid?(rest, crc),
         record <- Record.from_binary(rest) do
      {:reply, {:ok, record.value}, %{fd: fd}}
    else
      false ->
        {:reply, {:error, :corrupted_data}, %{fd: fd}}

      error ->
        {:reply, error, %{fd: fd}}
    end
  end

  defp is_valid?(data, crc) do
    decoded_crc = :binary.decode_unsigned(crc)
    :erlang.crc32(data) == decoded_crc
  end
end
