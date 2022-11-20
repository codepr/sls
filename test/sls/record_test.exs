defmodule Sls.RecordTest do
  @moduledoc false
  use ExUnit.Case, async: true

  describe "from_kv/2" do
    test "create a record from a key and a value" do
      assert %Sls.Record{key: key, value: value} = Sls.Record.from_kv("test_key", "test_value")
      assert key == "test_key"
      assert value == "test_value"
    end
  end

  describe "from_kv/1" do
    test "create a record from a key-value tuple" do
      assert %Sls.Record{key: key, value: value} = Sls.Record.from_kv({"test_key", "test_value"})
      assert key == "test_key"
      assert value == "test_value"
    end
  end

  describe "to_binary/1" do
    test "serialize to binary, returning size infos" do
      record = Sls.Record.from_kv({"test_key", "test_value"})

      assert %{
               binary_payload: _payload,
               key_size: 8,
               offset: 22,
               value_size: 10
             } = Sls.Record.to_binary(record)
    end
  end
end
