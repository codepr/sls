defmodule Sls.Reader do
  @moduledoc false
  use GenServer
  alias Sls.Index

  def start_link(opts \\ []) do
    log_path = Keyword.fetch!(opts, :log_path)
    table = Keyword.get(opts, :table, :index_map)
    GenServer.start_link(__MODULE__, %{log_path: log_path, table: table})
  end

  def get(key), do: get(__MODULE__, key)

  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  @impl true
  def init(%{log_path: log_path, table: table}) do
    fd = File.open!(log_path, [:read, :binary])
    {:ok, %{fd: fd, table: table}}
  end

  @impl true
  def handle_call({:get, key}, _from, %{fd: fd, table: table}) do
    case Index.lookup(table, key) do
      {:ok, {offset, size}} ->
        {:reply, :file.pread(fd, offset, size), %{fd: fd}}

      {:error, _} = error ->
        {:reply, error, %{fd: fd}}
    end
  end
end
