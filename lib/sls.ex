defmodule Sls do
  @moduledoc """
  Documentation for `Sls`.
  """
  use Application

  @log_path Application.compile_env!(:sls, :log_path)

  @doc """
  Hello world.

  ## Examples

      iex> Sls.hello()
      :world

  """
  @impl true
  def start(_type, _args) do
    Sls.Supervisor.start_link(log_path: @log_path, name: Sls.Supervisor)
  end

  defdelegate put(key, value), to: Sls.Writer
  defdelegate get(key), to: Sls.ReaderPool
end
