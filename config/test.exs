import Config

config :sls,
  reader_workers: 1,
  default_cache_table: :test_map,
  log_path: "./test/fixtures/test.db"
