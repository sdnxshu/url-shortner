import string
import random
import os
import time
import uuid

import structlog
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, HttpUrl

from logging_config import setup_logging
from database import SessionLocal
import models
import cache

# Configure structlog before anything else logs
setup_logging()
logger = structlog.get_logger()

RATE_LIMIT  = int(os.getenv("RATE_LIMIT",  10))
RATE_WINDOW = int(os.getenv("RATE_WINDOW", 60))

app = FastAPI(title="URL Shortener")


# ---------------------------------------------------------------------------
# Request tracing middleware — binds request_id + method + path to every log
# emitted during that request, then logs a summary line when it completes.
# ---------------------------------------------------------------------------

@app.middleware("http")
async def request_tracing_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    start = time.perf_counter()

    # Bind shared fields into the context-local logger for this request
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        request_id=request_id,
        method=request.method,
        path=request.url.path,
        client_ip=request.headers.get("X-Forwarded-For", request.client.host if request.client else "unknown"),
    )

    logger.info("request.started")

    response = await call_next(request)

    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    logger.info(
        "request.completed",
        status_code=response.status_code,
        duration_ms=duration_ms,
    )

    # Echo the request_id back to the caller
    response.headers["X-Request-ID"] = request_id
    return response


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

def get_client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def generate_code(length: int = 6) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


def flush_clicks_to_db(short_code: str):
    """Background task: drain buffered Redis clicks into Postgres."""
    delta = cache.flush_clicks(short_code)
    if delta:
        db = SessionLocal()
        try:
            entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
            if entry:
                entry.clicks += delta
                db.commit()
                logger.info("clicks.flushed", short_code=short_code, delta=delta)
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

@app.get("/health")
def health():
    redis_ok = cache.redis_healthy()
    logger.info("health.checked", redis_ok=redis_ok)
    return {
        "status": "ok",
        "redis": "up" if redis_ok else "degraded — running Postgres-only",
    }


@app.post("/shorten", response_model=ShortenResponse)
def shorten_url(req: ShortenRequest, request: Request, db: Session = Depends(get_db)):
    ip = get_client_ip(request)
    allowed, count, retry_after = cache.check_rate_limit(ip, RATE_LIMIT, RATE_WINDOW)

    if not allowed:
        logger.warning(
            "rate_limit.exceeded",
            ip=ip,
            limit=RATE_LIMIT,
            window=RATE_WINDOW,
            retry_after=retry_after,
        )
        raise HTTPException(
            status_code=429,
            detail=f"Too many requests. Try again in {retry_after}s.",
            headers={"Retry-After": str(retry_after)},
        )

    code = req.custom_code or generate_code()

    existing = db.query(models.URL).filter(models.URL.short_code == code).first()
    if existing:
        logger.warning("shorten.conflict", short_code=code)
        raise HTTPException(status_code=409, detail=f"Code '{code}' is already taken.")

    url_entry = models.URL(short_code=code, original_url=str(req.url))
    db.add(url_entry)
    db.commit()
    db.refresh(url_entry)

    cache.cache_url(code, str(req.url))

    logger.info("url.shortened", short_code=code, original_url=str(req.url), custom=bool(req.custom_code))

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
    original_url = cache.get_cached_url(short_code)

    if original_url:
        cache.increment_clicks(short_code)
        buffered = cache.get_buffered_clicks(short_code)
        if buffered >= 10:
            background_tasks.add_task(flush_clicks_to_db, short_code)
        logger.info("redirect.cache_hit", short_code=short_code)
        return RedirectResponse(url=original_url, status_code=302)

    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        logger.warning("redirect.not_found", short_code=short_code)
        raise HTTPException(status_code=404, detail="Short URL not found.")

    cache.cache_url(short_code, url_entry.original_url)
    url_entry.clicks += 1
    db.commit()

    logger.info("redirect.cache_miss", short_code=short_code)
    return RedirectResponse(url=url_entry.original_url, status_code=302)


@app.get("/stats/{short_code}")
def stats(short_code: str, db: Session = Depends(get_db)):
    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        logger.warning("stats.not_found", short_code=short_code)
        raise HTTPException(status_code=404, detail="Short URL not found.")

    buffered = cache.flush_clicks(short_code)
    if buffered:
        url_entry.clicks += buffered
        db.commit()

    logger.info("stats.served", short_code=short_code, clicks=url_entry.clicks)
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
        logger.warning("delete.not_found", short_code=short_code)
        raise HTTPException(status_code=404, detail="Short URL not found.")

    cache.invalidate_url(short_code)
    db.delete(url_entry)
    db.commit()

    logger.info("url.deleted", short_code=short_code)
    return {"message": f"'{short_code}' deleted successfully."}
