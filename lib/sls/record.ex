defmodule Sls.Record do
  @moduledoc false

  @enforce_keys [:key, :value]

  defstruct [:key, :value, :timestamp]

  @type t :: %__MODULE__{key: iodata(), value: iodata(), timestamp: non_neg_integer()}

  @header_size 14

  def from_kv({key, value}), do: from_kv(key, value)

  def from_kv(key, value),
    do: %__MODULE__{
      key: stringify(key),
      value: stringify(value),
      timestamp: :os.system_time(:millisecond)
    }

  def size, do: @header_size

  defp stringify(data) when is_bitstring(data), do: data
  defp stringify(data) when is_atom(data), do: Atom.to_string(data)
  defp stringify(data), do: Kernel.inspect(data)

  def to_binary(record) do
    record
    |> to_binary_record()
    |> add_crc32()
  end

  def from_binary(data) do
    <<timestamp::big-unsigned-integer-size(64), key_size::big-unsigned-integer-size(16),
      value_size::big-unsigned-integer-size(32), rest::binary>> = data

    <<key::binary-size(key_size), value::binary-size(value_size)>> = rest

    %__MODULE__{
      timestamp: timestamp,
      key: key,
      value: value
    }
  end

  defp to_binary_record(%{key: key, value: value, timestamp: timestamp}) do
    timestamp_bin = uint_to_bin(timestamp, 64)

    key_size = byte_size(key)
    value_size = byte_size(value)

    key_size_bin = uint_to_bin(key_size, 16)
    value_size_bin = uint_to_bin(value_size, 32)

    size_bin = <<key_size_bin::binary, value_size_bin::binary>>
    key_value_bin = <<key::binary, value::binary>>

    binary_record = <<timestamp_bin::binary, size_bin::binary, key_value_bin::binary>>

    %{
      binary_payload: binary_record,
      record_size: key_size + value_size + size()
    }
  end

  defp add_crc32(%{binary_payload: data} = record_map) do
    payload =
      data
      |> :erlang.crc32()
      |> uint_to_bin(32)

    Map.put(record_map, :crc, <<payload::binary>>)
  end

  defp uint_to_bin(data, size), do: <<data::big-unsigned-integer-size(size)>>
end
