defmodule Sls.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Sls.Writer, "./tmp/test.db"}
    ]

    opts = [strategy: :one_for_one, name: Sls.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
