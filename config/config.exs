import Config

config :sls,
  reader_workers: 10,
  default_cache_table: :index_map,
  log_path: "test.db"
