defmodule Sls.ReaderTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import PathHelpers

  alias Sls.Reader
  alias Sls.Writer

  @test_db "reader-test.db"
  @test_table :reader_test_table

  setup do
    with fixture <- fixture_path(@test_db),
         writer_pid <-
           start_supervised!(
             {Writer, log_path: fixture, name: :read_test_writer, table: @test_table}
           ),
         reader_pid <-
           start_supervised!(
             {Reader, log_path: fixture, table: @test_table, single_process: true}
           ),
         :ok <- on_exit(fn -> File.rm_rf!(fixture) end) do
      {:ok, writer_pid: writer_pid, reader_pid: reader_pid}
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

  describe "get/1" do
    test "retrieves offset and size from the index and data from the persistence", %{
      writer_pid: writer_pid,
      reader_pid: reader_pid
    } do
      assert {:ok, {0, 35}} = Writer.put(writer_pid, "test_key", "test_data")
      assert {:ok, "test_data"} = Reader.get(reader_pid, "test_key")
    end

    test "return :not_found error when a key is not found", %{
      reader_pid: reader_pid
    } do
      assert {:error, :not_found} = Reader.get(reader_pid, "non_existent_key")
    end

    test "return :corrupted_data error when crc doesn't match", %{
      reader_pid: reader_pid,
      writer_pid: writer_pid
    } do
      assert {:ok, {0, 35}} = Writer.put(writer_pid, "test_key", "test_data")
      # simulating data corruption by changing few bytes just after the header size
      @test_db
      |> fixture_path()
      |> pwrite(20, <<0, 15, 10>>)

      assert {:error, :corrupted_data} = Reader.get(reader_pid, "test_key")
    end
  end
end
