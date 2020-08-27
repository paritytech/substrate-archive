-- trigger for block
CREATE TRIGGER new_block_trigger
    AFTER INSERT
    ON blocks
    FOR EACH ROW
    EXECUTE PROCEDURE table_update_trigger_fn('blocks_update')
