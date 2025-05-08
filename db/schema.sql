CREATE SCHEMA IF NOT EXISTS roster_reconciler;

CREATE TABLE IF NOT EXISTS roster_reconciler.roster_reconciler_runs (
  run_id uuid PRIMARY KEY,
  app_name text NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz NOT NULL,
  previous_path text NOT NULL,
  current_path text NOT NULL,
  key_columns text NOT NULL,
  key_normalize text NOT NULL,
  value_normalize text NOT NULL,
  summary_only boolean NOT NULL,
  detail_limit integer,
  total_previous integer NOT NULL,
  total_current integer NOT NULL,
  added integer NOT NULL,
  removed integer NOT NULL,
  updated integer NOT NULL,
  unchanged integer NOT NULL,
  duplicate_keys_previous integer NOT NULL,
  duplicate_keys_current integer NOT NULL,
  invalid_rows_previous integer NOT NULL,
  invalid_rows_current integer NOT NULL,
  net_change integer NOT NULL,
  shared_count integer NOT NULL,
  added_columns text,
  removed_columns text,
  ignored_fields text,
  unknown_ignored_fields text,
  export_dir text,
  export_unchanged boolean NOT NULL,
  export_updated_rows boolean NOT NULL,
  export_status boolean NOT NULL,
  json_path text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS roster_reconciler_runs_started_at_idx
  ON roster_reconciler.roster_reconciler_runs (started_at);
