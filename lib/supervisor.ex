defmodule Sls.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts \\ []) do
    log_path = Keyword.fetch!(opts, :log_path)
    Supervisor.start_link(__MODULE__, log_path, opts)
  end

  @impl true
  def init(log_path) do
    children = [
      {Sls.Writer, log_path: log_path},
      Supervisor.child_spec({Sls.Reader, log_path: log_path}, id: :reader_1),
      Supervisor.child_spec({Sls.Reader, log_path: log_path}, id: :reader_2)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
