defmodule Sls.RecordTest do
  @moduledoc false
  use ExUnit.Case, async: true

  describe "from_kv/2" do
    test "create a record from a key and a value" do
      assert %Sls.Record{key: key, value: value} = Sls.Record.from_kv("test_key", "test_value")
      assert key == "test_key"
      assert value == "test_value"
    end

    test "stringify key and value" do
      %{key: key, value: value} = Sls.Record.from_kv(:test_key, 12)
      assert key == "test_key"
      assert value == "12"
    end
  end

  describe "from_kv/1" do
    test "create a record from a key-value tuple" do
      assert %Sls.Record{key: key, value: value} = Sls.Record.from_kv({"test_key", "test_value"})
      assert key == "test_key"
      assert value == "test_value"
    end
  end

  describe "to_binary/1-from_binary/1" do
    test "serialize to binary, returning size infos" do
      record = Sls.Record.from_kv({"test_key", "test_value"})

      assert %{
               binary_payload: payload,
               record_size: 32,
               crc: _crc
             } = Sls.Record.to_binary(record)

      assert ^record = Sls.Record.from_binary(payload)
    end
  end
end
