defmodule Sls.IndexTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Sls.Index

  @test_table :test_table

  setup do
    Index.init(@test_table)
    :ok
  end

  describe "insert/3" do
    test "inserts a key in the index" do
      assert :ok = Index.insert(@test_table, "test_key", 10, 15)
      assert {:ok, {10, 15}} = Index.lookup(@test_table, "test_key")
    end

    test "inserts a key if it exists already" do
      with :ok <- Index.insert(@test_table, "test_key", 10, 15),
           :ok <- Index.insert(@test_table, "test_key", 15, 15) do
        assert {:ok, {15, 15}} = Index.lookup(@test_table, "test_key")
      end
    end
  end

  describe "lookup/1" do
    test "returns the offset and size of a key" do
      :ok = Index.insert(@test_table, "test_key", 10, 15)
      assert {:ok, {10, 15}} = Index.lookup(@test_table, "test_key")
    end

    test "returns a :not_found error if a key is not found in the index" do
      :ok = Index.insert(@test_table, "test_key", 10, 15)
      assert {:error, :not_found} = Index.lookup(@test_table, "test_key_2")
    end
  end
end