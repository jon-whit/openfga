-- +goose Up
ALTER TABLE IF EXISTS tuple
  ADD COLUMN condition_name TEXT,
  ADD COLUMN condition_context JSONB;

-- +goose Down
ALTER TABLE IF EXISTS tuple
  DROP COLUMN IF EXISTS condition_context,
  DROP COLUMN IF EXISTS condition_name;