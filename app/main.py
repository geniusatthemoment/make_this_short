import os
import random
import string
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, HttpUrl
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import Boolean, DateTime, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://myappuser:StrongPostgresPass123@127.0.0.1:5432/myappdb",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://:StrongRedisPassword@127.0.0.1:6379/0")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1").rstrip("/")

REQUEST_COUNT = Counter("http_requests_total", "Total HTTP requests", ["method", "path", "status"])
REQUEST_LATENCY = Histogram("http_request_duration_seconds", "Request latency", ["method", "path"])
LINKS_CREATED = Counter("links_created_total", "Total created short links")
REDIRECTS_TOTAL = Counter("redirects_total", "Total successful redirects")
REDIRECT_ERRORS = Counter("redirect_errors_total", "Total redirect errors", ["reason"])
CACHE_HITS = Counter("cache_hits_total", "Redis cache hits")
CACHE_MISSES = Counter("cache_misses_total", "Redis cache misses")
ACTIVE_LINKS = Gauge("active_links", "Number of active links")


class Base(DeclarativeBase):
    pass


class Link(Base):
    __tablename__ = "links"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    original_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    short_code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    click_count: Mapped[int] = mapped_column(Integer, default=0)


engine = create_async_engine(DATABASE_URL, future=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
redis_client: Optional[redis.Redis] = None


class CreateLinkRequest(BaseModel):
    original_url: HttpUrl
    custom_code: Optional[str] = None


class LinkResponse(BaseModel):
    original_url: str
    short_code: str
    short_url: str
    click_count: int
    is_active: bool


async def get_or_create_code(session: AsyncSession, requested: Optional[str] = None) -> str:
    if requested:
        existing = await session.scalar(select(Link).where(Link.short_code == requested))
        if existing:
            raise HTTPException(status_code=409, detail="custom_code already exists")
        return requested

    while True:
        code = "".join(random.choices(string.ascii_letters + string.digits, k=6))
        existing = await session.scalar(select(Link).where(Link.short_code == code))
        if not existing:
            return code


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    yield
    if redis_client:
        await redis_client.close()
    await engine.dispose()


app = FastAPI(title="shortener-service", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    path = request.url.path
    method = request.method
    start = time.perf_counter()
    status = "500"
    try:
        response = await call_next(request)
        status = str(response.status_code)
        return response
    finally:
        REQUEST_COUNT.labels(method=method, path=path, status=status).inc()
        REQUEST_LATENCY.labels(method=method, path=path).observe(time.perf_counter() - start)


@app.get("/")
async def root():
    return {"service": "shortener", "status": "ok"}


@app.get("/health")
async def health():
    try:
        async with SessionLocal() as session:
            await session.execute(select(1))
        if redis_client:
            await redis_client.ping()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"unhealthy: {e}")


@app.get("/metrics")
async def metrics():
    async with SessionLocal() as session:
        result = await session.execute(select(Link).where(Link.is_active == True))
        ACTIVE_LINKS.set(len(result.scalars().all()))
    return JSONResponse(content=generate_latest().decode("utf-8"), media_type=CONTENT_TYPE_LATEST)


@app.post("/links", response_model=LinkResponse)
async def create_link(payload: CreateLinkRequest):
    async with SessionLocal() as session:
        code = await get_or_create_code(session, payload.custom_code)
        link = Link(original_url=str(payload.original_url), short_code=code)
        session.add(link)
        await session.commit()
        await session.refresh(link)
        if redis_client:
            await redis_client.setex(f"link:{code}", 3600, str(payload.original_url))
        LINKS_CREATED.inc()
        return LinkResponse(
            original_url=link.original_url,
            short_code=link.short_code,
            short_url=f"{BASE_URL}/{link.short_code}",
            click_count=link.click_count,
            is_active=link.is_active,
        )


@app.get("/links/{short_code}", response_model=LinkResponse)
async def get_link(short_code: str):
    async with SessionLocal() as session:
        link = await session.scalar(select(Link).where(Link.short_code == short_code))
        if not link:
            raise HTTPException(status_code=404, detail="link not found")
        return LinkResponse(
            original_url=link.original_url,
            short_code=link.short_code,
            short_url=f"{BASE_URL}/{link.short_code}",
            click_count=link.click_count,
            is_active=link.is_active,
        )


@app.get("/{short_code}")
async def redirect_to_original(short_code: str):
    if short_code in {"health", "metrics", "links"}:
        raise HTTPException(status_code=404, detail="not found")

    cached_url = None
    if redis_client:
        cached_url = await redis_client.get(f"link:{short_code}")

    async with SessionLocal() as session:
        if cached_url:
            CACHE_HITS.inc()
            link = await session.scalar(select(Link).where(Link.short_code == short_code))
            if not link or not link.is_active:
                REDIRECT_ERRORS.labels(reason="inactive_or_missing").inc()
                raise HTTPException(status_code=404, detail="link not found")
            link.click_count += 1
            await session.commit()
            REDIRECTS_TOTAL.inc()
            return RedirectResponse(url=cached_url, status_code=307)

        CACHE_MISSES.inc()
        link = await session.scalar(select(Link).where(Link.short_code == short_code))
        if not link:
            REDIRECT_ERRORS.labels(reason="not_found").inc()
            raise HTTPException(status_code=404, detail="link not found")
        if not link.is_active:
            REDIRECT_ERRORS.labels(reason="inactive").inc()
            raise HTTPException(status_code=410, detail="link inactive")

        link.click_count += 1
        await session.commit()
        if redis_client:
            await redis_client.setex(f"link:{short_code}", 3600, link.original_url)
        REDIRECTS_TOTAL.inc()
        return RedirectResponse(url=link.original_url, status_code=307)
