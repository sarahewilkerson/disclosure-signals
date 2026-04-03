"""
REST API for the Insider Trading Signal Engine.

Provides read-only endpoints for querying signals, scores, and aggregate indices.

Usage:
    uvicorn api:app --host 0.0.0.0 --port 8000
    # Or with reload for development:
    uvicorn api:app --reload
"""

import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import get_connection, init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    # Startup: initialize database
    init_db()
    yield
    # Shutdown: nothing to clean up


# Initialize FastAPI app
app = FastAPI(
    title="Insider Trading Signal Engine",
    description="REST API for querying insider trading signals derived from SEC Form 4 filings",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Pydantic models for response schemas
# ---------------------------------------------------------------------------
class HealthResponse(BaseModel):
    status: str
    database: str
    timestamp: str


class CompanyScore(BaseModel):
    cik: str
    ticker: str
    company_name: Optional[str] = None
    sector: Optional[str] = None
    signal: str
    score: float
    confidence: float
    confidence_tier: str
    buy_count: int
    sell_count: int
    unique_buyers: int
    unique_sellers: int
    net_buy_value: float
    window_days: int
    computed_at: str
    explanation: Optional[str] = None


class CompanyScoreList(BaseModel):
    scores: list[CompanyScore]
    total: int
    window_days: int


class AggregateIndex(BaseModel):
    window_days: int
    computed_at: str
    risk_appetite_index: float
    bullish_breadth: float
    bearish_breadth: float
    neutral_pct: float
    insufficient_pct: float
    ceo_cfo_only_index: Optional[float] = None
    sector_balanced_index: Optional[float] = None
    cyclical_score: Optional[float] = None
    defensive_score: Optional[float] = None
    sector_breakdown: Optional[dict] = None
    total_companies: int
    companies_with_signal: int


class StatusResponse(BaseModel):
    companies: int
    filings: int
    transactions: int
    signal_transactions: int
    company_scores: int
    filing_date_range: Optional[dict] = None
    last_computed: Optional[str] = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """
    Health check endpoint.

    Returns the API status and database connection status.
    """
    db_status = "unknown"
    try:
        with get_connection() as conn:
            conn.execute("SELECT 1")
            db_status = "connected"
    except Exception:
        db_status = "error"

    return HealthResponse(
        status="healthy" if db_status == "connected" else "unhealthy",
        database=db_status,
        timestamp=datetime.now().isoformat(),
    )


@app.get("/status", response_model=StatusResponse, tags=["System"])
async def get_status():
    """
    Get database and pipeline status.

    Returns counts of companies, filings, transactions, and scores.
    """
    with get_connection() as conn:
        companies = conn.execute("SELECT COUNT(*) as c FROM companies").fetchone()["c"]
        filings = conn.execute("SELECT COUNT(*) as c FROM filings").fetchone()["c"]
        txns = conn.execute("SELECT COUNT(*) as c FROM transactions").fetchone()["c"]
        signal_txns = conn.execute(
            "SELECT COUNT(*) as c FROM transactions WHERE include_in_signal = 1"
        ).fetchone()["c"]
        scores = conn.execute("SELECT COUNT(*) as c FROM company_scores").fetchone()["c"]

        filing_date_range = None
        if filings > 0:
            latest = conn.execute("SELECT MAX(filing_date) as d FROM filings").fetchone()["d"]
            oldest = conn.execute("SELECT MIN(filing_date) as d FROM filings").fetchone()["d"]
            filing_date_range = {"oldest": oldest, "latest": latest}

        last_computed = conn.execute(
            "SELECT MAX(computed_at) as c FROM company_scores"
        ).fetchone()["c"]

        return StatusResponse(
            companies=companies,
            filings=filings,
            transactions=txns,
            signal_transactions=signal_txns,
            company_scores=scores,
            filing_date_range=filing_date_range,
            last_computed=last_computed,
        )


@app.get("/scores", response_model=CompanyScoreList, tags=["Scores"])
async def get_scores(
    window_days: int = Query(default=90, description="Lookback window in days"),
    signal: Optional[str] = Query(default=None, description="Filter by signal (BULLISH, BEARISH, NEUTRAL, INSUFFICIENT)"),
    sector: Optional[str] = Query(default=None, description="Filter by sector"),
    min_confidence: Optional[float] = Query(default=None, ge=0, le=1, description="Minimum confidence threshold"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum results to return"),
    offset: int = Query(default=0, ge=0, description="Result offset for pagination"),
):
    """
    Get company scores.

    Returns a list of company scores for the specified lookback window,
    optionally filtered by signal type, sector, or minimum confidence.
    """
    with get_connection() as conn:
        query = """
            SELECT cs.*, c.company_name, c.sector
            FROM company_scores cs
            LEFT JOIN companies c ON cs.cik = c.cik
            WHERE cs.window_days = ?
        """
        params = [window_days]

        if signal:
            query += " AND cs.signal = ?"
            params.append(signal.upper())

        if sector:
            query += " AND c.sector = ?"
            params.append(sector)

        if min_confidence is not None:
            query += " AND cs.confidence >= ?"
            params.append(min_confidence)

        # Get total count
        count_query = query.replace("SELECT cs.*, c.company_name, c.sector", "SELECT COUNT(*) as total")
        total = conn.execute(count_query, params).fetchone()["total"]

        # Add ordering and pagination
        query += " ORDER BY ABS(cs.score) DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = conn.execute(query, params).fetchall()

        scores = []
        for row in rows:
            scores.append(CompanyScore(
                cik=row["cik"],
                ticker=row["ticker"],
                company_name=row["company_name"],
                sector=row["sector"],
                signal=row["signal"],
                score=row["score"],
                confidence=row["confidence"],
                confidence_tier=row["confidence_tier"],
                buy_count=row["buy_count"],
                sell_count=row["sell_count"],
                unique_buyers=row["unique_buyers"],
                unique_sellers=row["unique_sellers"],
                net_buy_value=row["net_buy_value"],
                window_days=row["window_days"],
                computed_at=row["computed_at"],
                explanation=row["explanation"],
            ))

        return CompanyScoreList(
            scores=scores,
            total=total,
            window_days=window_days,
        )


@app.get("/scores/{ticker}", response_model=CompanyScore, tags=["Scores"])
async def get_score_by_ticker(
    ticker: str,
    window_days: int = Query(default=90, description="Lookback window in days"),
):
    """
    Get score for a specific company by ticker.

    Returns the score for the specified ticker and lookback window.
    """
    with get_connection() as conn:
        row = conn.execute("""
            SELECT cs.*, c.company_name, c.sector
            FROM company_scores cs
            LEFT JOIN companies c ON cs.cik = c.cik
            WHERE cs.ticker = ? AND cs.window_days = ?
        """, (ticker.upper(), window_days)).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Score not found for ticker {ticker}")

        return CompanyScore(
            cik=row["cik"],
            ticker=row["ticker"],
            company_name=row["company_name"],
            sector=row["sector"],
            signal=row["signal"],
            score=row["score"],
            confidence=row["confidence"],
            confidence_tier=row["confidence_tier"],
            buy_count=row["buy_count"],
            sell_count=row["sell_count"],
            unique_buyers=row["unique_buyers"],
            unique_sellers=row["unique_sellers"],
            net_buy_value=row["net_buy_value"],
            window_days=row["window_days"],
            computed_at=row["computed_at"],
            explanation=row["explanation"],
        )


@app.get("/aggregate", response_model=AggregateIndex, tags=["Aggregate"])
async def get_aggregate_index(
    window_days: int = Query(default=90, description="Lookback window in days"),
):
    """
    Get the aggregate market sentiment index.

    Returns the overall market sentiment derived from aggregating
    all company signals for the specified lookback window.
    """
    with get_connection() as conn:
        row = conn.execute("""
            SELECT * FROM aggregate_index WHERE window_days = ?
        """, (window_days,)).fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"Aggregate index not found for window_days={window_days}"
            )

        # Parse sector_breakdown JSON if present
        sector_breakdown = None
        if row["sector_breakdown"]:
            try:
                sector_breakdown = json.loads(row["sector_breakdown"])
            except (json.JSONDecodeError, TypeError):
                pass

        return AggregateIndex(
            window_days=row["window_days"],
            computed_at=row["computed_at"],
            risk_appetite_index=row["risk_appetite_index"],
            bullish_breadth=row["bullish_breadth"],
            bearish_breadth=row["bearish_breadth"],
            neutral_pct=row["neutral_pct"],
            insufficient_pct=row["insufficient_pct"],
            ceo_cfo_only_index=row["ceo_cfo_only_index"],
            sector_balanced_index=row["sector_balanced_index"],
            cyclical_score=row["cyclical_score"],
            defensive_score=row["defensive_score"],
            sector_breakdown=sector_breakdown,
            total_companies=row["total_companies"],
            companies_with_signal=row["companies_with_signal"],
        )


@app.get("/sectors", tags=["Reference"])
async def get_sectors():
    """
    Get list of all sectors in the universe.

    Returns a list of unique sector names for filtering.
    """
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT DISTINCT sector FROM companies WHERE sector IS NOT NULL ORDER BY sector
        """).fetchall()

        return {"sectors": [row["sector"] for row in rows]}


@app.get("/companies", tags=["Reference"])
async def get_companies(
    sector: Optional[str] = Query(default=None, description="Filter by sector"),
    limit: int = Query(default=100, ge=1, le=500, description="Maximum results"),
):
    """
    Get list of companies in the universe.

    Returns company CIKs and tickers for reference.
    """
    with get_connection() as conn:
        query = "SELECT cik, ticker, company_name, sector, fortune_rank FROM companies"
        params = []

        if sector:
            query += " WHERE sector = ?"
            params.append(sector)

        query += " ORDER BY fortune_rank LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()

        return {
            "companies": [
                {
                    "cik": row["cik"],
                    "ticker": row["ticker"],
                    "company_name": row["company_name"],
                    "sector": row["sector"],
                    "fortune_rank": row["fortune_rank"],
                }
                for row in rows
            ],
            "total": len(rows),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
