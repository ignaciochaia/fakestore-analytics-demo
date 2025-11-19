# FakeStore → Supabase ETL demo for Metabase dashboards

This repository contains a simple Python ETL that loads data from the FakeStore API into an existing Supabase Postgres database so Metabase dashboards can point at clean tables and views.

## Prerequisites

- Python 3.11+
- Supabase project already created with the required `products`, `customers`, `carts`, and `cart_items` tables

## Local development

1. (Optional) Create and activate a virtual environment.
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in a valid `PG_CONN_STR`. If direct access to fakestoreapi.com is blocked (e.g., on CI runners), set `FAKESTORE_BASE_URL` to a proxy like `https://r.jina.ai/https://fakestoreapi.com`.
4. Export `PG_CONN_STR` (and optional overrides) or load via your preferred tool: `export PG_CONN_STR=...`
5. Run the ETL: `python etl_fakestore.py`

## GitHub Actions setup

1. Push this repository to GitHub.
2. In the repository, open **Settings → Secrets → Actions**.
3. Add a new secret named `PG_CONN_STR` containing your Supabase connection string.
4. The provided workflow (`.github/workflows/fakestore-etl.yml`) will use this secret when running on the nightly schedule or via manual dispatch.
