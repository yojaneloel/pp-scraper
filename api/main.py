# api/main.py
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import text

from shared.db import SessionLocal

app = FastAPI(title="PP‑Scraper API")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/domains")
async def list_domains(db=Depends(get_db)):
    rows = db.execute(text(
        "SELECT DISTINCT domain FROM policies ORDER BY domain"
    )).fetchall()
    return [r.domain for r in rows]

@app.get("/policies/{domain}")
async def get_policies(domain: str, db=Depends(get_db)):
    rows = db.execute(text(
        "SELECT id, fetched_at FROM policies WHERE domain=:d ORDER BY fetched_at DESC"
    ), {"d": domain}).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Domain not found")
    return [
        {"id": r.id, "fetched_at": r.fetched_at.isoformat()}
        for r in rows
    ]

@app.get("/diff/{diff_id}")
async def get_diff(diff_id: int, db=Depends(get_db)):
    row = db.execute(text(
        "SELECT diff_json, change_pct, section_changes, semantic_flags "
        "FROM diffs WHERE id=:i"
    ), {"i": diff_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Diff not found")
    return {
        "change_pct": row.change_pct,
        "diff": row.diff_json,
        "section_changes": row.section_changes,
        "semantic_flags": row.semantic_flags,
    }

# New endpoint: list semantic flags over time for a domain
@app.get("/diffs/{domain}/flags")
async def list_semantic_flags(domain: str, db=Depends(get_db)):
    query = text(
        "SELECT d.id AS diff_id, d.created_at, d.semantic_flags "
        "FROM diffs d "
        "JOIN policies p ON d.policy_id_new = p.id "
        "WHERE p.domain = :d "
        "ORDER BY d.created_at DESC"
    )
    rows = db.execute(query, {"d": domain}).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No diffs found for domain")
    return [
        {"diff_id": r.diff_id, "timestamp": r.created_at.isoformat(), "flags": r.semantic_flags}
        for r in rows
    ]