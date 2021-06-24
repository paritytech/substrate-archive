-- General function that can be used for any table
CREATE OR REPLACE FUNCTION table_update_trigger_fn()
   RETURNS TRIGGER
   LANGUAGE PLPGSQL
AS $BODY$
DECLARE
  channel TEXT := TG_ARGV[0];
  block_num JSON;
  notification JSON;
BEGIN

    IF (TG_OP = 'DELETE') THEN
      block_num = OLD.block_num;
    ELSE
      block_num = NEW.block_num;
    END IF;

    -- create json payload
     notification := json_build_object(
        'table',TG_TABLE_NAME,
        'action', TG_OP,
        'block_num', block_num
    );

    PERFORM pg_notify(channel, notification::TEXT);
    RETURN NULL;
END;
$BODY$



