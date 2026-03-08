import os
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TTL = int(os.getenv("CACHE_TTL", 3600))  # seconds; default 1 hour

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(REDIS_URL, decode_responses=True)
    return _client


# ---------- URL cache ----------

def cache_url(short_code: str, original_url: str) -> None:
    get_redis().setex(f"url:{short_code}", TTL, original_url)


def get_cached_url(short_code: str) -> str | None:
    return get_redis().get(f"url:{short_code}")


def invalidate_url(short_code: str) -> None:
    get_redis().delete(f"url:{short_code}", f"stats:{short_code}")


# ---------- Click counter (write-back buffer) ----------

def increment_clicks(short_code: str) -> int:
    """Increment an in-Redis click counter and return the new value."""
    return get_redis().incr(f"clicks:{short_code}")


def flush_clicks(short_code: str) -> int:
    """Atomically read + reset the buffered click counter. Returns delta."""
    key = f"clicks:{short_code}"
    delta = get_redis().getdel(key)
    return int(delta) if delta else 0