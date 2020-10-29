-- General function that can be used for any table
CREATE OR REPLACE FUNCTION table_update_trigger_fn()
   RETURNS TRIGGER
   LANGUAGE PLPGSQL
AS $BODY$
DECLARE
  channel TEXT := TG_ARGV[0];
  id JSON;
  notification JSON;
BEGIN

    IF (TG_OP = 'DELETE') THEN
      id = OLD.id;
    ELSE
      id = NEW.id;
    END IF;

    -- create json payload
     notification := json_build_object(
        'table',TG_TABLE_NAME,
        'action', TG_OP,
        'id', id
    );

    PERFORM pg_notify(channel, notification::TEXT);
    RETURN NULL;
END;
$BODY$


