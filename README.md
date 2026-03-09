# URL Shortener

A simple URL shortener built with **FastAPI** + **PostgreSQL** (via Docker).

## Quick Start

```bash
docker compose up --build
```

API will be available at `http://localhost:8000`  
Interactive docs at `http://localhost:8000/docs`

---

## Endpoints

### `POST /shorten` — Shorten a URL
```json
{ "url": "https://example.com/very/long/path" }
```
Optional custom code:
```json
{ "url": "https://example.com", "custom_code": "mylink" }
```

### `GET /{short_code}` — Redirect to original URL

### `GET /stats/{short_code}` — View click stats

### `DELETE /{short_code}` — Delete a short URL

---

## Example with curl

```bash
# Shorten
curl -X POST http://localhost:8000/shorten \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.google.com"}'

# Redirect (follow with -L)
curl -L http://localhost:8000/abc123

# Stats
curl http://localhost:8000/stats/abc123
```
