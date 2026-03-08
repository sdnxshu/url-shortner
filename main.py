from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from pydantic import BaseModel, HttpUrl
import string, random

from database import SessionLocal, engine
import models

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="URL Shortener")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def generate_code(length: int = 6) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choices(chars, k=length))


class ShortenRequest(BaseModel):
    url: HttpUrl
    custom_code: str | None = None


class ShortenResponse(BaseModel):
    short_code: str
    short_url: str
    original_url: str


@app.post("/shorten", response_model=ShortenResponse)
def shorten_url(req: ShortenRequest, db: Session = Depends(get_db)):
    code = req.custom_code or generate_code()

    # Check if code already exists
    existing = db.query(models.URL).filter(models.URL.short_code == code).first()
    if existing:
        raise HTTPException(status_code=409, detail=f"Code '{code}' is already taken.")

    url_entry = models.URL(short_code=code, original_url=str(req.url))
    db.add(url_entry)
    db.commit()
    db.refresh(url_entry)

    return ShortenResponse(
        short_code=code,
        short_url=f"http://localhost:8000/{code}",
        original_url=str(req.url),
    )


@app.get("/{short_code}")
def redirect(short_code: str, db: Session = Depends(get_db)):
    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        raise HTTPException(status_code=404, detail="Short URL not found.")

    url_entry.clicks += 1
    db.commit()

    return RedirectResponse(url=url_entry.original_url, status_code=302)


@app.get("/stats/{short_code}")
def stats(short_code: str, db: Session = Depends(get_db)):
    url_entry = db.query(models.URL).filter(models.URL.short_code == short_code).first()
    if not url_entry:
        raise HTTPException(status_code=404, detail="Short URL not found.")

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

    db.delete(url_entry)
    db.commit()
    return {"message": f"'{short_code}' deleted successfully."}