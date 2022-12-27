defmodule Sls.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(opts) do
    table = Keyword.fetch!(opts, :table)
    path = Keyword.fetch!(opts, :path)
    Supervisor.start_link(__MODULE__, %{path: path, table: table}, opts)
  end

  @impl true
  def init(%{path: path, table: table}) do
    children = [
      {Sls.Writer, path: path, table: table},
      {Sls.ReaderPool, path: path, table: table}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
