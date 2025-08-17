# diff/main.py
import os
import json
import difflib
import logging
from datetime import datetime, timezone
import boto3
from sqlalchemy import text
from shared.db import SessionLocal
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("diff")

# AWS & SQL setup
BUCKET = os.getenv("S3_BUCKET")
REGION = os.getenv("AWS_REGION", "us-east-1")
s3 = boto3.client("s3", region_name=REGION)

SQL_UNDIFFED = text("""
-- find new versions that need diffs
SELECT p_new.id   AS new_id,
       p_prev.id  AS prev_id,
       p_new.storage_path AS new_key,
       p_prev.storage_path AS prev_key
FROM policies p_new
JOIN policies p_prev
  ON p_new.domain = p_prev.domain
 AND p_prev.fetched_at = (
     SELECT MAX(fetched_at)
     FROM policies
     WHERE domain = p_new.domain
       AND fetched_at < p_new.fetched_at
 )
LEFT JOIN diffs d
  ON d.policy_id_new = p_new.id
WHERE d.id IS NULL;
""")

SQL_INSERT = text("""
-- insert enriched diff record
INSERT INTO diffs (
  policy_id_prev,
  policy_id_new,
  diff_json,
  change_pct,
  section_changes,
  semantic_flags,
  created_at
) VALUES (
  :prev,
  :new,
  :diff,
  :pct,
  :sections,
  :flags,
  :created_at
);
""")

def split_sections(text: str):
    """Split based on Markdown headings (# through ######)."""
    lines = text.splitlines(keepends=True)
    sections = []
    title = None
    content = []
    for line in lines:
        m = re.match(r'^(#{1,6})\s*(.+)', line)
        if m:
            if title is not None:
                sections.append({"title": title, "content": "".join(content)})
            title = m.group(2).strip()
            content = []
        else:
            if title is None:
                title = "<untitled>"
            content.append(line)
    if title is not None:
        sections.append({"title": title, "content": "".join(content)})
    return sections

def detect_section_changes(old: str, new: str, threshold: float = 0.99):
    """Return lists of added, removed, and modified section titles."""
    old_map = {s["title"]: s["content"] for s in split_sections(old)}
    new_map = {s["title"]: s["content"] for s in split_sections(new)}

    added = [t for t in new_map if t not in old_map]
    removed = [t for t in old_map if t not in new_map]
    modified = []
    for t in set(old_map) & set(new_map):
        sm = difflib.SequenceMatcher(
            None,
            old_map[t].splitlines(),
            new_map[t].splitlines()
        )
        if sm.ratio() < threshold:
            modified.append(t)

    return {"added": added, "removed": removed, "modified": modified}

def detect_semantic_flags(text: str):
    """Flag key privacy terms/patterns."""
    flags = {"data_sale": bool(
        re.search(r"\b(?:sell(?:ing)? user data|data sale)\b", text, re.IGNORECASE)
    ), "retention_period": bool(
        re.search(r"\bretain.*?for\s+\d+\s+(?:days|months|years)\b", text, re.IGNORECASE)
    ), "erasure_right": bool(
        re.search(r"\b(?:right to erasure|right to be forgotten|erasure)\b", text, re.IGNORECASE)
    ), "data_portability": bool(
        re.search(r"\b(?:data portability|portability)\b", text, re.IGNORECASE)
    ), "cookies_tracking": bool(
        re.search(r"\b(?:cookie|cookies|tracking|web beacon)\b", text, re.IGNORECASE)
    )}
    return flags

def fetch_text(key: str) -> str:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read().decode("utf-8")

def run_diff():
    with SessionLocal() as db:
        rows = db.execute(SQL_UNDIFFED).fetchall()
        for r in rows:
            old = fetch_text(r.prev_key)
            new = fetch_text(r.new_key)

            # overall diff
            sm = difflib.SequenceMatcher(None, old.splitlines(), new.splitlines())
            pct = 100 * (1 - sm.ratio())
            ops = sm.get_opcodes()

            # detailed analysis
            sections = detect_section_changes(old, new)
            flags = detect_semantic_flags(new)

            db.execute(SQL_INSERT, {
                "prev": r.prev_id,
                "new": r.new_id,
                "diff": json.dumps(ops),
                "pct": pct,
                "sections": json.dumps(sections),
                "flags": json.dumps(flags),
                "created_at": datetime.now(timezone.utc),
            })
            logger.info(f"Diff {r.prev_id}->{r.new_id}: {pct:.2f}%")

        db.commit()

if __name__ == "__main__":
    run_diff()