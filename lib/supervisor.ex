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
      {Sls.ReaderPool, log_path: log_path}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
