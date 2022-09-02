-- trigger for block
CREATE TRIGGER new_trex_trigger
    AFTER INSERT
    ON trex
    FOR EACH ROW
    EXECUTE PROCEDURE table_update_trigger_fn('trex_update')