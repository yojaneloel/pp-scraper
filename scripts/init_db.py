from shared.db import engine
from sqlalchemy import text

# DDL statements to create tables if they do not exist
DDL = """
CREATE TABLE IF NOT EXISTS policies (
  id              SERIAL PRIMARY KEY,
  domain          TEXT NOT NULL,
  url             TEXT NOT NULL,
  fetched_at      TIMESTAMPTZ NOT NULL,
  version_hash    TEXT NOT NULL,
  storage_path    TEXT NOT NULL,
  status_code     SMALLINT NOT NULL,
  content_type    TEXT,
  text_length     INT NOT NULL
);

CREATE TABLE IF NOT EXISTS diffs (
  id              SERIAL PRIMARY KEY,
  policy_id_prev  INT REFERENCES policies(id),
  policy_id_new   INT REFERENCES policies(id),
  diff_json       JSONB NOT NULL,
  change_pct      NUMERIC(5,2) NOT NULL,
  section_changes JSONB,
  semantic_flags  JSONB,
  created_at      TIMESTAMPTZ NOT NULL
);
"""

with engine.begin() as conn:
    conn.execute(text(DDL))
    print("✅ Database schema initialized.")