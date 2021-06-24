-- Converts varchars to integers. Use LTRIM if varcahr contains '0x'

CREATE OR REPLACE FUNCTION hex_to_int(hexval varchar) RETURNS integer AS $$
DECLARE
	result bigint;
BEGIN
	EXECUTE 'SELECT x' || quote_literal(hexval) || '::bigint' INTO result;
	RETURN result;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
