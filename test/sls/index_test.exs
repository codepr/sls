defmodule Sls.IndexTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Sls.Index

  setup do
    {:ok, _pid} = Index.start_link([])
    :ok
  end

  describe "upsert/3" do
    test "inserts a key in the index" do
      assert :ok = Index.upsert("test_key", 10, 15)
      assert {:ok, {10, 15}} = Index.lookup("test_key")
    end

    test "upserts a key if it exists already" do
      with :ok <- Index.upsert("test_key", 10, 15),
           :ok <- Index.upsert("test_key", 15, 15) do
        assert {:ok, {15, 15}} = Index.lookup("test_key")
      end
    end
  end

  describe "lookup/1" do
    test "returns the offset and size of a key" do
      :ok = Index.upsert("test_key", 10, 15)
      assert {:ok, {10, 15}} = Index.lookup("test_key")
    end

    test "returns a :not_found error if a key is not found in the index" do
      :ok = Index.upsert("test_key", 10, 15)
      assert {:error, :not_found} = Index.lookup("test_key_2")
    end
  end
end
