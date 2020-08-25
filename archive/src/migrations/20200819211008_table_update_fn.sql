-- General function that can be used for any table
CREATE OR REPLACE FUNCTION table_update_trigger_fn()
   RETURNS TRIGGER
   LANGUAGE PLPGSQL
AS $BODY$
DECLARE
  channel TEXT := TG_ARGV[0];
  data JSON;
  notification JSON;
BEGIN

    IF (TG_OP = 'DELETE') THEN
      data = row_to_json(OLD);
    ELSE
      data = row_to_json(NEW);
    END IF;

    -- create json payload
     notification := json_build_object(
        'table',TG_TABLE_NAME,
        'action', TG_OP,
        'data', data::TEXT
    );

    PERFORM pg_notify(channel, notification::TEXT);
    RETURN NULL;
END;
$BODY$


