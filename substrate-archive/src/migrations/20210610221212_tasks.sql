TRUNCATE TABLE _background_tasks;

-- Change data type of column 'data' to JSONB from BYTEA.
ALTER TABLE _background_tasks
DROP COLUMN data;

ALTER TABLE _background_tasks
ADD COLUMN "data" jsonb;
