defmodule Sls.WriterTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import PathHelpers

  alias Sls.Writer

  @test_db "writer-test.db"
  @test_table :writer_test_table

  setup do
    with fixture <- fixture_path(@test_db),
         writer_pid <-
           start_supervised!(
             {Writer, log_path: fixture, name: :writer_test_writer, table: @test_table}
           ),
         :ok <- on_exit(fn -> File.rm_rf!(fixture) end) do
      {:ok, writer_pid: writer_pid}
    else
      error ->
        @test_db
        |> fixture_path()
        |> rm_rf!()

        raise error
    end
  rescue
    error ->
      @test_db
      |> fixture_path()
      |> rm_rf!()

      raise error
  end

  describe "put/2" do
    test "stores offset and size in memory and persist data", %{writer_pid: writer_pid} do
      assert {:ok, {0, 35}} = Writer.put(writer_pid, "test_key", "test_data")
    end

    test "raise on tombstone value as value", %{writer_pid: writer_pid} do
      assert_raise RuntimeError, "Tombstone value not valid as value", fn ->
        Writer.put(writer_pid, "test_key", Writer.tombstone())
      end
    end
  end

  describe "delete/1" do
    test "put a tombstone value to delete a key", %{writer_pid: writer_pid} do
      assert {:ok, {0, 39}} = Writer.delete(writer_pid, "test-key")
    end
  end
end
