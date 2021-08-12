CREATE TABLE IF NOT EXISTS _sa_config (
  id SERIAL NOT NULL,  
  task_queue VARCHAR NOT NULL,
  last_run TIMESTAMPTZ NOT NULL,
  last_version real NOT NULL
);
