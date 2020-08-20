-- General function that can be used for any table
CREATE OR REPLACE FUNCTION table_update_trigger_fn()
   RETURNS TRIGGER
   LANGUAGE PGPLSQL
AS $BODY$
DECLARE
  data JSON;
  notification JSON;
BEGIN
    IF (TG_OP = 'DELETE') THEN
      data = row_to_json(OLD);
    ELSE
      data = row_to_json(NEW);
    ENDIF

    -- create json payload
     notification = json_build_object(
        'table',TG_TABLE_NAME,
        'action', TG_OP,
        'data', data
    );

    PERFORM pg_notify('table_update', notification::TEXT);
END;
$BODY$


