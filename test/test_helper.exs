apps_to_start = [:mox]
Enum.each(apps_to_start, &Application.ensure_all_started/1)
ExUnit.start()

defmodule PathHelpers do
  def fixture_path do
    Path.expand("fixtures", __DIR__)
  end

  def fixture_path(extra) do
    Path.join(fixture_path(), extra)
  end

  def rm_rf!(path) do
    if File.exists?(path), do: File.rm_rf!(path)
  end

  def pwrite(path, position, bytes) do
    path
    |> File.open!([:write, :binary])
    |> :file.pwrite(position, bytes)
  end
end
