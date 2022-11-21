defmodule Sls.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts) do
    table = Keyword.fetch!(opts, :table)
    log_path = Keyword.fetch!(opts, :log_path)
    Supervisor.start_link(__MODULE__, %{log_path: log_path, table: table}, opts)
  end

  @impl true
  def init(%{log_path: log_path, table: table}) do
    children = [
      {Sls.Writer, log_path: log_path, table: table},
      {Sls.ReaderPool, log_path: log_path, table: table}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
