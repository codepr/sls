defmodule Sls.ReaderTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import PathHelpers

  alias Sls.Reader
  alias Sls.Writer

  @test_db "test.db"

  setup do
    with fixture <- fixture_path(@test_db),
         writer_pid <- start_supervised!({Writer, log_path: fixture}),
         reader_pid <- start_supervised!({Reader, log_path: fixture}),
         :ok <-
           on_exit(fn -> File.rm_rf!(fixture) end) do
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
      assert {:ok, {22, 9}} = Writer.put(writer_pid, "test_key", "test_data")
      assert {:ok, "test_data"} = Reader.get(reader_pid, "test_key")
    end
  end
end
