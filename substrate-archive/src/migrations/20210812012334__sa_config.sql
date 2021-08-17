CREATE TABLE IF NOT EXISTS _sa_config (
  id SERIAL NOT NULL,  
  task_queue VARCHAR NOT NULL,
  last_run TIMESTAMPTZ NOT NULL,
  major integer NOT NULL,
  minor integer NOT NULL,
  patch integer NOT NULL
);
