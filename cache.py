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


# ---------- Sliding window rate limiter ----------

def check_rate_limit(identifier: str, limit: int, window_seconds: int) -> tuple[bool, int, int]:
    """
    Sliding window rate limiter using a Redis sorted set.

    Stores each request as a member with score = timestamp (ms).
    Trims entries outside the window, then checks count vs limit.

    Returns:
        (allowed, current_count, retry_after_seconds)
    """
    import time

    r = get_redis()
    now_ms = int(time.time() * 1000)
    window_ms = window_seconds * 1000
    key = f"ratelimit:{identifier}"

    pipe = r.pipeline()
    # Remove requests outside the sliding window
    pipe.zremrangebyscore(key, 0, now_ms - window_ms)
    # Count remaining requests in window
    pipe.zcard(key)
    # Add current request
    pipe.zadd(key, {str(now_ms): now_ms})
    # Set TTL so key auto-expires
    pipe.expire(key, window_seconds + 1)
    results = pipe.execute()

    current_count = results[1]  # count BEFORE adding current request

    if current_count >= limit:
        # Find the oldest entry to tell caller when window clears
        oldest = r.zrange(key, 0, 0, withscores=True)
        retry_after = 0
        if oldest:
            oldest_ms = int(oldest[0][1])
            retry_after = max(0, window_seconds - (now_ms - oldest_ms) // 1000)
        return False, current_count, retry_after

    return True, current_count + 1, 0