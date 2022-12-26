defmodule Sls.DataFile do
  @moduledoc false

  @enforce_keys [:id, :fd, :path, :readonly?]

  defstruct @enforce_keys

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          fd: File.io_device(),
          path: String.t(),
          readonly?: boolean()
        }

  def open!(id, path, readonly?), do: open!(%{id: id, path: path, readonly?: readonly?})

  def open!(%{id: id, path: path, readonly?: readonly?}) do
    modes = if readonly?, do: [:read, :binary], else: [:read, :write, :binary]
    fd = File.open!(path, modes)

    %__MODULE__{
      id: id,
      path: path,
      readonly?: readonly?,
      fd: fd
    }
  end

  def append(%{fd: fd} = datafile, data) do
    case IO.binwrite(fd, data) do
      :ok -> {:ok, %{datafile | fd: fd}}
      error -> error
    end
  end

  def read_at(%{fd: fd} = datafile, offset, bytes) do
    case :file.pread(fd, offset, bytes) do
      {:ok, data} -> {:ok, {data, %{datafile | fd: fd}}}
      other -> other
    end
  end

  def close(%{fd: fd}) do
    File.close(fd)
  end
end
