from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, HttpUrl
import string, random

from database import SessionLocal, engine
import models
import cache

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="URL Shortener")


# ---------------------------------------------------------------------------
# DB dependency
# ---------------------------------------------------------------------------

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def generate_code(length: int = 6) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


def flush_clicks_to_db(short_code: str):
    """Background task: drain buffered Redis clicks into Postgres."""
    delta = cache.flush_clicks(short_code)
    if delta:
        db = SessionLocal()
        try:
            url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
            if url_entry:
                url_entry.clicks += delta
                db.commit()
        finally:
            db.close()


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class ShortenRequest(BaseModel):
    url: HttpUrl
    custom_code: str | None = None


class ShortenResponse(BaseModel):
    short_code: str
    short_url: str
    original_url: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/shorten", response_model=ShortenResponse)
def shorten_url(req: ShortenRequest, db: Session = Depends(get_db)):
    code = req.custom_code or generate_code()

    existing = db.query(models.URL).filter(models.URL.short_code == code).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Code '{code}' is already taken.")

    url_entry = models.URL(short_code=code, original_url=str(req.url))
    db.add(url_entry)
    db.commit()
    db.refresh(url_entry)

    # Warm the cache immediately after creation
    cache.cache_url(code, str(req.url))

    return ShortenResponse(
        short_code=code,
        short_url=f"http://localhost:8000/{code}",
        original_url=str(req.url),
    )


@app.get("/{short_code}")
def redirect(
    short_code: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    # 1. Cache hit — fast path, no DB query needed
    original_url = cache.get_cached_url(short_code)
    if original_url:
        cache.increment_clicks(short_code)
        # Flush buffered clicks to Postgres every 10 hits
        buffered = int(cache.get_redis().get(f"clicks:{short_code}") or 0)
        if buffered >= 10:
            background_tasks.add_task(flush_clicks_to_db, short_code)
        return RedirectResponse(url=original_url, status_code=302)

    # 2. Cache miss — hit Postgres and populate cache
    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    cache.cache_url(short_code, url_entry.original_url)
    url_entry.clicks += 1
    db.commit()

    return RedirectResponse(url=url_entry.original_url, status_code=302)


@app.get("/stats/{short_code}")
def stats(short_code: str, db: Session = Depends(get_db)):
    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    # Add any buffered (not-yet-flushed) clicks for an accurate live count
    buffered = cache.flush_clicks(short_code)
    if buffered:
        url_entry.clicks += buffered
        db.commit()

    return {
        "short_code": url_entry.short_code,
        "original_url": url_entry.original_url,
        "clicks": url_entry.clicks,
        "created_at": url_entry.created_at,
    }


@app.delete("/{short_code}")
def delete_url(short_code: str, db: Session = Depends(get_db)):
    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    cache.invalidate_url(short_code)   # evict from cache first
    db.delete(url_entry)
    db.commit()
    return {"message": f"'{short_code}' deleted successfully."}