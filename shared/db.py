# shared/db.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

# DATABASE_URL example: postgresql+psycopg2://user:pass@hostname:5432/dbname
engine = create_engine(
    os.environ.get("DATABASE_URL"),
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False
)