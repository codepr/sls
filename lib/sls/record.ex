defmodule Sls.Record do
  @moduledoc false

  @enforce_keys [:key, :value]

  defstruct [:key, :value, :timestamp]

  @type t :: %__MODULE__{key: iodata(), value: iodata(), timestamp: non_neg_integer()}

  def from_kv(key, value),
    do: %__MODULE__{
      key: stringify(key),
      value: stringify(value),
      timestamp: :os.system_time(:millisecond)
    }

  defp stringify(data) when is_bitstring(data), do: data
  defp stringify(data) when is_atom(data), do: Atom.to_string(data)
  defp stringify(data), do: Kernel.inspect(data)

  def from_kv({key, value}), do: from_kv(key, value)

  def to_binary(%{key: key, value: value, timestamp: timestamp}) do
    timestamp_bin = uint_to_bin(timestamp, 64)

    key_size = byte_size(key)
    value_size = byte_size(value)

    key_size_bin = uint_to_bin(key_size, 16)
    value_size_bin = uint_to_bin(value_size, 32)

    size_bin = <<key_size_bin::binary, value_size_bin::binary>>
    key_value_bin = <<key::binary, value::binary>>

    binary_record = <<timestamp_bin::binary, size_bin::binary, key_value_bin::binary>>
    value_rel_offset = byte_size(timestamp_bin) + byte_size(size_bin) + key_size

    %{
      binary_payload: binary_record,
      key_size: key_size,
      offset: value_rel_offset,
      value_size: value_size
    }
  end

  defp uint_to_bin(data, size), do: <<data::big-unsigned-integer-size(size)>>
end
