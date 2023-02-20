# Sls

Simple [Bitcask](https://riak.com/assets/bitcask-intro.pdf) implementation using
`GenServer`.

The key benefits are:

- Low latency per item read or written
- Consistent performance
- Handles datasets larger than RAM
- Small design specification

The main drawback is:

- All your keys must fit in RAM

GenServers are used to both write and read to the datafiles, using a single
GenServer process to write and multiple GenServers to read. The in-memory
index is based on the `ets` cache.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `sls` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:sls, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/sls>.

