defmodule Sls do
  @moduledoc """
  Documentation for `Sls`.
  """
  use Application

  @doc """
  Hello world.

  ## Examples

      iex> Sls.hello()
      :world

  """
  @impl true
  def start(_type, _args) do
    Sls.Supervisor.start_link(log_path: "test.db", name: Sls.Supervisor)
  end

  defdelegate put(key, value), to: Sls.Writer
  defdelegate get(key), to: Sls.Reader
end
