defmodule Sls.IndexTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Sls.Index

  @warm_up_test_table :index_test_table
  @test_table :index_test_table

  describe "insert/3" do
    setup do
      Index.init(%{}, table: @test_table)
      :ok
    end

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
    setup do
      Index.init(%{}, table: @test_table)
      :ok
    end

    test "returns the offset and size of a key" do
      :ok = Index.insert(@test_table, "test_key", 10, 15)
      assert {:ok, {10, 15}} = Index.lookup(@test_table, "test_key")
    end

    test "returns a :not_found error if a key is not found in the index" do
      :ok = Index.insert(@test_table, "test_key", 10, 15)
      assert {:error, :not_found} = Index.lookup(@test_table, "test_key_2")
    end
  end

  describe "delete/1" do
    setup do
      Index.init(%{}, table: @test_table)
      :ok
    end

    test "delete an existing key" do
      :ok = Index.insert(@test_table, "test_key", 10, 15)
      assert {:ok, {10, 15}} = Index.lookup(@test_table, "test_key")
      :ok = Index.delete(@test_table, "test_key")
      assert {:error, :not_found} = Index.lookup(@test_table, "test_key")
    end
  end

  describe "init/1" do
    test "warm cache up at start" do
      Index.init(%{"dummy_key_2" => {54, 7}}, table: @warm_up_test_table)
      assert {:ok, {54, 7}} = Index.lookup(@warm_up_test_table, "dummy_key_2")
      assert {:error, :not_found} = Index.lookup(@warm_up_test_table, "test_key_2")
    end
  end
end
