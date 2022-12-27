defmodule Sls do
  @moduledoc """
  Documentation for `Sls`.
  """
  use Application

  @table Application.compile_env!(:sls, :default_cache_table)
  @path Application.compile_env!(:sls, :path)

  @doc """
  Hello world.

  ## Examples

      iex> Sls.hello()
      :world

  """
  @impl true
  def start(_type, _args) do
    Sls.Supervisor.start_link(path: @path, table: @table, name: Sls.Supervisor)
  end

  defdelegate put(key, value), to: Sls.Writer
  defdelegate get(key), to: Sls.ReaderPool
  defdelegate delete(key), to: Sls.Writer
end
