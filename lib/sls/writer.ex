defmodule Sls.Writer do
  @moduledoc false
  use GenServer
  alias Sls.Index
  alias Sls.Record

  def start_link(opts \\ []) do
    log_path = Keyword.fetch!(opts, :log_path)
    GenServer.start_link(__MODULE__, log_path, name: __MODULE__)
  end

  def put(key, value), do: put(__MODULE__, key, value)

  def put(pid, key, value) do
    GenServer.call(pid, {:put, key, value})
  end

  @impl true
  def init(log_path) do
    Index.init()
    fd = File.open!(log_path, [:write, :binary])
    {:ok, %{fd: fd, current_offset: 0}}
  end

  @impl true
  def handle_call({:put, key, value}, _from, %{fd: fd, current_offset: current_offset} = state) do
    %{binary_payload: payload, offset: offset, value_size: value_size} =
      {key, value}
      |> Record.from_kv()
      |> Record.to_binary()

    :ok = IO.binwrite(fd, payload)
    value_offset = current_offset + offset
    Index.insert(key, value_offset, value_size)

    {:reply, {:ok, {value_offset, value_size}},
     %{state | current_offset: value_offset + value_size}}
  end

  @impl true
  def terminate(reason, _state) do
    Index.shutdown()
    IO.puts("Terminating: #{reason}")
  end
end
