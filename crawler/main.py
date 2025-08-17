# crawler/main.py
import os
import time
import hashlib
from datetime import datetime, timezone
import logging
import requests
import boto3
from sqlalchemy import text

from shared.db import SessionLocal

# Configure logging
title = "crawler"
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(title)

# Load environment variables
BUCKET = os.getenv("S3_BUCKET")
REGION = os.getenv("AWS_REGION", "us-east-1")
# Hard‑coded domains for MVP; replace with DB or file later
domains = ["example.com", "cnn.com"]

# Initialize AWS and HTTP clients
s3 = boto3.client("s3", region_name=REGION)
session = requests.Session()
session.headers.update({"User-Agent": "PPBot/0.1"})

# SQL for inserting a new policy version
SQL_INSERT = text(
    """
    INSERT INTO policies (
        domain, url, fetched_at, version_hash, storage_path,
        status_code, content_type, text_length
    ) VALUES (
        :domain, :url, :fetched_at, :hash,
        :storage_path, :status, :ctype, :length
    ) RETURNING id
    """
)

# Crawl each domain once per run
def crawl():
    with SessionLocal() as db:
        for domain in domains:
            url = f"https://{domain}/privacy"
            logger.info(f"Fetching {url}")
            try:
                resp = session.get(url, timeout=20)
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                continue

            text_body = resp.text
            # Compute content hash
            digest = hashlib.sha256(text_body.encode("utf-8")).hexdigest()
            # S3 key: raw/<domain>/YYYY/MM/DD/HHMMSS.html
            timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
            key = f"raw/{domain}/{timestamp}.html"

            # Upload raw text to S3
            try:
                s3.put_object(Bucket=BUCKET, Key=key, Body=text_body.encode("utf-8"))
                logger.info(f"Uploaded to s3://{BUCKET}/{key}")
            except Exception as e:
                logger.error(f"S3 upload failed for {key}: {e}")
                continue

            # Insert metadata row into Postgres
            try:
                result = db.execute(
                    SQL_INSERT,
                    {
                        "domain": domain,
                        "url": url,
                        "fetched_at": datetime.now(timezone.utc),
                        "hash": digest,
                        "storage_path": key,
                        "status": resp.status_code,
                        "ctype": resp.headers.get("Content-Type", ""),
                        "length": len(text_body),
                    },
                )
                new_id = result.scalar_one()
                db.commit()
                logger.info(f"Inserted policy row id={new_id}")
            except Exception as e:
                logger.error(f"DB insert failed for {domain}: {e}")
                db.rollback()
            # Politeness delay
            time.sleep(1)

if __name__ == "__main__":
    crawl()
