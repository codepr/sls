import Config

config :sls,
  reader_workers: 10,
  default_cache_table: :index_map,
  path: "test.db"
