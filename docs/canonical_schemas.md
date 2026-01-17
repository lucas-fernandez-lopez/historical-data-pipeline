# Canonical Historical Data Schemas

This document defines the canonical schemas used to store historical crypto data
across all data sources. These schemas are shared by both historical backfill
and future live ingestion pipelines.

All canonical schemas are source-agnostic and time-standardized.

---

## Design Principles

- All timestamps are stored in UTC
- Time granularity is explicit (e.g. daily)
- Raw source data is stored separately and mapped into canonical schemas
- Canonical schemas represent the single source of truth for analytics

---

## Core Entities

### Assets

Defines the universe of assets supported by the system.

**Table:** `assets`

| Column        | Type   | Description |
|--------------|--------|-------------|
| asset_id     | text   | Canonical internal asset identifier (e.g. `btc`, `eth`) |
| symbol       | text   | Exchange-facing symbol (e.g. `BTC`) |
| asset_type   | text   | Asset classification (e.g. `crypto`) |
| created_at   | timestamptz | Record creation time (UTC) |

**Primary Key:** `asset_id`

---

### Prices (Daily)

Canonical daily OHLCV price data aligned across all sources.

**Table:** `prices_daily`

| Column      | Type   | Description |
|------------|--------|-------------|
| asset_id   | text   | Canonical asset identifier |
| day        | date   | Trading day (UTC) |
| high       | numeric | Highest price |
| low        | numeric | Lowest price |
| volume     | numeric | Trading volume |
| source     | text   | Data source identifier |
| ingested_at | timestamptz | Ingestion timestamp |

**Primary Key:** `(asset_id, day, source)`

---

## Schema Versioning

Canonical schemas are versioned through additive, backward-compatible changes.
Breaking changes require explicit migration and documentation.
