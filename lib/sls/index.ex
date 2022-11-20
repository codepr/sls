defmodule Sls.Index do
  @moduledoc false

  @table :index_map

  def init(opts \\ []) do
    table = Keyword.get(opts, :table, @table)

    with log_path <- Keyword.fetch!(opts, :log_path),
         {:ok, fd} <- File.open(log_path, [:read, :binary]),
         {_current_offset, offsets} <- load_offsets(fd) do
      File.close(fd)
      :ets.new(table, [:named_table, :protected, read_concurrency: true])
      :ets.insert(table, Map.to_list(offsets))
    else
      _ -> :ets.new(table, [:named_table, :protected, read_concurrency: true])
    end
  end

  def insert(table, key, offset, size) do
    :ets.insert(table, {key, {offset, size}})
    :ok
  end

  def insert(key, offset, size), do: insert(@table, key, offset, size)

  def lookup(table, key) do
    case :ets.lookup(table, key) do
      [{^key, {offset, size}}] -> {:ok, {offset, size}}
      [] -> {:error, :not_found}
    end
  end

  def lookup(key), do: lookup(@table, key)

  def shutdown(table), do: :ets.delete(table)
  def shutdown, do: shutdown(@table)

  defp load_offsets(fd, offsets \\ %{}, current_offset \\ 0) do
    :file.position(fd, current_offset)

    with <<_timestamp::big-unsigned-integer-size(64)>> <- IO.binread(fd, 8),
         <<key_size::big-unsigned-integer-size(16)>> <- IO.binread(fd, 2),
         <<value_size::big-unsigned-integer-size(32)>> <- IO.binread(fd, 4),
         key <- IO.binread(fd, key_size) do
      value_obs_offset = current_offset + 14 + key_size
      offsets = Map.put(offsets, key, {value_obs_offset, value_size})
      load_offsets(fd, offsets, value_obs_offset + value_size)
    else
      :eof -> {current_offset, offsets}
    end
  end
end
