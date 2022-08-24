-- Add migration script here
ALTER TABLE state_traces ALTER COLUMN trace_id TYPE bigint; 
ALTER TABLE state_traces ALTER COLUMN trace_parent_id TYPE bigint; 