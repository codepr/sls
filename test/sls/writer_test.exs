defmodule Sls.WriterTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import Mox, only: [verify_on_exit!: 1]
  import PathHelpers

  alias Sls.Writer

  setup :verify_on_exit!

  @test_db "test.db"

  setup do
    with fixture <- fixture_path(@test_db),
         writer_pid <- start_supervised!({Writer, log_path: fixture}),
         :ok <- on_exit(fn -> File.rm_rf!(fixture) end) do
      {:ok, writer_pid: writer_pid}
    else
      _error ->
        @test_db
        |> fixture_path()
        |> rm_rf!()
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
      assert {:ok, {0, 9}} = Writer.put(writer_pid, "test_key", "test_data")
    end
  end
end
