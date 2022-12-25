defmodule Sls.Index do
  @moduledoc false

  @table Application.compile_env!(:sls, :default_cache_table)
  @header_size 14

  def init(opts \\ []) do
    table = Keyword.get(opts, :table, @table)

    with log_path <- Keyword.fetch!(opts, :log_path),
         {:ok, fd} <- File.open(log_path, [:read, :binary]),
         {_current_offset, offsets} <- load_offsets(fd) do
      File.close(fd)
      warm_up(table, offsets)
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

  def delete(table, key) do
    :ets.delete(table, key)
    :ok
  end

  def shutdown(table), do: :ets.delete(table)
  def shutdown, do: shutdown(@table)

  defp warm_up(table, offsets) do
    table
    |> :ets.new([:named_table, :protected, read_concurrency: true])
    |> :ets.insert(Map.to_list(offsets))
  end

  defp load_offsets(fd, offsets \\ %{}, current_offset \\ 0) do
    :file.position(fd, current_offset)

    with <<_timestamp::big-unsigned-integer-size(64), key_size::big-unsigned-integer-size(16),
           value_size::big-unsigned-integer-size(32)>> <- IO.binread(fd, @header_size),
         key <- IO.binread(fd, key_size) do
      value_obs_offset = current_offset + @header_size + key_size
      offsets = Map.put(offsets, key, {value_obs_offset, value_size})
      load_offsets(fd, offsets, value_obs_offset + value_size)
    else
      :eof ->
        {current_offset, offsets}
    end
  end
end
