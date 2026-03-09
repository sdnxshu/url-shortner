import os
import time
import logging

import redis

logger = logging.getLogger("cache")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TTL = int(os.getenv("CACHE_TTL", 3600))       # seconds; default 1 hour
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 0.1                           # seconds between retries

_client: redis.Redis | None = None
_redis_healthy = True                          # module-level circuit flag


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
            retry_on_timeout=False,
        )
    return _client


def _is_available() -> bool:
    """Quick PING to check whether Redis is reachable right now."""
    global _redis_healthy
    try:
        get_redis().ping()
        if not _redis_healthy:
            logger.info("Redis is back online — resuming cache.")
        _redis_healthy = True
        return True
    except Exception:
        if _redis_healthy:
            logger.warning("Redis is unavailable — falling back to Postgres-only mode.")
        _redis_healthy = False
        return False


def _safe(fn, *args, default=None, **kwargs):
    """
    Call a Redis function with automatic retries.
    Returns `default` (None) on any error so callers can degrade gracefully.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except redis.RedisError as exc:
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY * attempt)
            else:
                logger.warning("Redis call failed after %d attempts: %s", RETRY_ATTEMPTS, exc)
                global _redis_healthy
                _redis_healthy = False
    return default


# ---------------------------------------------------------------------------
# Health check (used by /health endpoint)
# ---------------------------------------------------------------------------

def redis_healthy() -> bool:
    return _is_available()


# ---------------------------------------------------------------------------
# URL cache
# ---------------------------------------------------------------------------

def cache_url(short_code: str, original_url: str) -> None:
    _safe(get_redis().setex, f"url:{short_code}", TTL, original_url)


def get_cached_url(short_code: str) -> str | None:
    return _safe(get_redis().get, f"url:{short_code}")


def invalidate_url(short_code: str) -> None:
    _safe(get_redis().delete, f"url:{short_code}", f"stats:{short_code}")


# ---------------------------------------------------------------------------
# Click counter (write-back buffer)
# ---------------------------------------------------------------------------

def increment_clicks(short_code: str) -> int:
    result = _safe(get_redis().incr, f"clicks:{short_code}", default=0)
    return int(result)


def get_buffered_clicks(short_code: str) -> int:
    result = _safe(get_redis().get, f"clicks:{short_code}", default=0)
    return int(result) if result else 0


def flush_clicks(short_code: str) -> int:
    """Atomically read + reset the buffered click counter. Returns delta."""
    result = _safe(get_redis().getdel, f"clicks:{short_code}", default=None)
    return int(result) if result else 0


# ---------------------------------------------------------------------------
# Sliding window rate limiter
# ---------------------------------------------------------------------------

def check_rate_limit(
    identifier: str, limit: int, window_seconds: int
) -> tuple[bool, int, int]:
    """
    Sliding window rate limiter using a Redis sorted set.

    Returns:
        (allowed, current_count, retry_after_seconds)

    If Redis is unavailable the limiter fails open (returns allowed=True)
    so a Redis outage never blocks legitimate traffic.
    """
    if not _is_available():
        logger.warning("Rate limiter skipped — Redis unavailable, failing open.")
        return True, 0, 0

    now_ms = int(time.time() * 1000)
    window_ms = window_seconds * 1000
    key = f"ratelimit:{identifier}"

    def _run_pipeline():
        r = get_redis()
        pipe = r.pipeline()
        pipe.zremrangebyscore(key, 0, now_ms - window_ms)
        pipe.zcard(key)
        pipe.zadd(key, {str(now_ms): now_ms})
        pipe.expire(key, window_seconds + 1)
        return pipe.execute()

    results = _safe(_run_pipeline, default=None)
    if results is None:
        # Redis failed mid-request — fail open
        return True, 0, 0

    current_count = results[1]  # count BEFORE this request was added

    if current_count >= limit:
        oldest = _safe(get_redis().zrange, key, 0, 0, withscores=True, default=[])
        retry_after = 0
        if oldest:
            oldest_ms = int(oldest[0][1])
            retry_after = max(0, window_seconds - (now_ms - oldest_ms) // 1000)
        return False, current_count, retry_after

    return True, current_count + 1, 0