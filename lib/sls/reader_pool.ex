defmodule Sls.ReaderPool do
  @moduledoc false
  use Supervisor

  alias Sls.Reader

  @workers Application.compile_env!(:sls, :reader_workers)

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts)
  end

  @impl true
  def init(opts) do
    workers = Keyword.get(opts, :workers, @workers)

    workers_specs =
      for index <- 1..workers do
        Supervisor.child_spec({Reader, opts}, id: {Reader, index})
      end

    workers_supervisor_specs = %{
      id: :workers_supervisor,
      type: :supervisor,
      start: {Supervisor, :start_link, [workers_specs, [strategy: :one_for_one]]}
    }

    children = [
      {Registry, name: __MODULE__, keys: :duplicate},
      workers_supervisor_specs
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def get(key) do
    readers = Registry.lookup(__MODULE__, :readers)
    {pid, _value = nil} = Enum.random(readers)
    Reader.get(pid, key)
  end

  def broadcast_new_datafile(log_path) do
    readers = Registry.lookup(__MODULE__, :readers)
    Enum.each(readers, fn pid -> Reader.add_new_datafile(pid, log_path) end)
  end
end
