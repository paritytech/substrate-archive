-- trigger for block
CREATE TRIGGER new_block_trigger
    AFTER INSERT
    ON blocks
    FOR EACH ROW
    EXECUTE PROCEDURE new_block_trigger_fn()
