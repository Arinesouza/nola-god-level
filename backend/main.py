# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any, Dict
import os
import sqlalchemy
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, Table

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+asyncpg://challenge:challenge_2024@localhost:5432/challenge_db")

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
metadata = MetaData()

app = FastAPI(title="Nola Analytics - Minimal Backend")

TABLE_CACHE: Dict[str, Table] = {}

class Metric(BaseModel):
    op: str
    column: Optional[str] = None
    alias: Optional[str] = None

class FilterItem(BaseModel):
    column: str
    op: str
    value: Any

class QueryRequest(BaseModel):
    table: str
    group_by: Optional[List[str]] = None
    metrics: Optional[List[Metric]] = None
    filters: Optional[List[FilterItem]] = None
    limit: Optional[int] = 1000

async def get_table(table_name: str) -> Table:
    if table_name in TABLE_CACHE:
        return TABLE_CACHE[table_name]
    async with engine.begin() as conn:
        local_meta = MetaData()
        tbl = Table(table_name, local_meta, autoload_with=conn)
        TABLE_CACHE[table_name] = tbl
        return tbl

def _apply_filter_clause(tbl: Table, f: FilterItem):
    col = tbl.c.get(f.column)
    if col is None:
        raise HTTPException(status_code=400, detail=f"Unknown column: {f.column}")
    op = f.op.lower()
    if op in ("=", "eq"):
        return col == f.value
    if op in ("!=", "ne"):
        return col != f.value
    if op == ">":
        return col > f.value
    if op == "<":
        return col < f.value
    if op == ">=":
        return col >= f.value
    if op == "<=":
        return col <= f.value
    if op == "in":
        if not isinstance(f.value, list):
            raise HTTPException(status_code=400, detail="Value for 'in' must be a list")
        return col.in_(f.value)
    if op == "like":
        return col.like(str(f.value))
    raise HTTPException(status_code=400, detail=f"Unsupported filter op: {f.op}")

@app.get("/health")
async def health():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/query")
async def query(req: QueryRequest):
    try:
        tbl = await get_table(req.table)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Table not found or reflection error: {e}")

    group_cols = []
    if req.group_by:
        for g in req.group_by:
            # allow passing raw SQL expressions like date_trunc via text if needed
            if "(" in g or "::" in g or g.count(" ")>0:
                # raw expression
                group_cols.append(text(g))
                continue
            c = tbl.c.get(g)
            if c is None:
                raise HTTPException(status_code=400, detail=f"Unknown group_by column: {g}")
            group_cols.append(c)

    sel_cols = []
    if req.metrics:
        for m in req.metrics:
            op = m.op.lower()
            if op == "count":
                expr = func.count() if not m.column else func.count(tbl.c.get(m.column))
            else:
                if not m.column:
                    raise HTTPException(status_code=400, detail=f"Metric {m.op} needs a column")
                col = tbl.c.get(m.column)
                if col is None:
                    raise HTTPException(status_code=400, detail=f"Unknown metric column: {m.column}")
                if op == "sum":
                    expr = func.sum(col)
                elif op == "avg":
                    expr = func.avg(col)
                elif op == "max":
                    expr = func.max(col)
                elif op == "min":
                    expr = func.min(col)
                else:
                    raise HTTPException(status_code=400, detail=f"Unsupported metric op: {m.op}")
            alias = m.alias or f"{op}_{m.column or 'all'}"
            sel_cols.append(expr.label(alias))

    if not sel_cols:
        sel_cols = [func.count().label("count")]

    stmt = select(*group_cols, *sel_cols).select_from(tbl)

    if req.filters:
        clauses = [_apply_filter_clause(tbl, FilterItem(**f.dict())) for f in req.filters]
        for c in clauses:
            stmt = stmt.where(c)

    if group_cols:
        stmt = stmt.group_by(*group_cols)

    if req.limit:
        stmt = stmt.limit(req.limit)

    async with AsyncSessionLocal() as session:
        try:
            res = await session.execute(stmt)
            rows = [dict(r) for r in res.fetchall()]
            return {"rows": rows}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/top-products")
async def top_products(store_id: Optional[int] = None, channel: Optional[str] = None, limit: int = 10):
    table_candidates = ["sales", "orders", "vendas"]
    tbl = None
    for name in table_candidates:
        try:
            tbl = await get_table(name)
            break
        except Exception:
            continue
    if tbl is None:
        raise HTTPException(status_code=404, detail="No sales table found. Edit table_candidates in code if needed.")
    cols = tbl.c
    if "product_id" not in cols:
        raise HTTPException(status_code=400, detail="sales table missing product_id column in this schema")

    price_col = None
    for name in ("total", "total_price", "price", "valor"):
        if name in cols:
            price_col = cols.get(name)
            break

    stmt = select(cols.product_id, func.count().label("qty"), (func.sum(price_col) if price_col is not None else func.sum(text("1"))).label("revenue")).group_by(cols.product_id).order_by(sqlalchemy.desc("qty")).limit(limit)
    if store_id is not None and "store_id" in cols:
        stmt = stmt.where(cols.store_id == store_id)
    if channel is not None and "channel" in cols:
        stmt = stmt.where(cols.channel == channel)

    async with AsyncSessionLocal() as session:
        res = await session.execute(stmt)
        rows = [dict(r) for r in res.fetchall()]
        return {"rows": rows}

@app.get("/repeat-customers")
async def repeat_customers(min_purchases: int = 3, days_since: int = 30):
    table_candidates = ["sales", "orders", "vendas"]
    tbl = None
    for name in table_candidates:
        try:
            tbl = await get_table(name)
            break
        except Exception:
            continue
    if tbl is None:
        raise HTTPException(status_code=404, detail="No sales/orders table found; edit table_candidates in code if needed")
    cols = tbl.c
    if "customer_id" not in cols or "created_at" not in cols:
        raise HTTPException(status_code=400, detail="schema must have customer_id and created_at on the sales table for this endpoint")

    subq = select(tbl.c.customer_id, func.count().label("cnt"), func.max(tbl.c.created_at).label("last_at")).group_by(tbl.c.customer_id).having(func.count() >= min_purchases).subquery()
    stmt = select(subq.c.customer_id, subq.c.cnt, subq.c.last_at).where(subq.c.last_at < func.now() - text(f"interval '{days_since} days'")).limit(1000)

    async with AsyncSessionLocal() as session:
        res = await session.execute(stmt)
        return {"rows": [dict(r) for r in res.fetchall()]}

@app.get("/")
def root():
    return {"app": "Nola Analytics - Minimal Backend", "usage": "POST /query to run pivot-like queries. See /docs for examples."}
