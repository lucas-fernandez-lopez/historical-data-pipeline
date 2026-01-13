# Historical Data Pipeline (Draft)
This repository contains a design-first proposal for a historical crypto data pipeline.

## Scope (current)
- Define canonical historical data schemas
- Align raw, normalized, and analytics layers
- Support both historical backfill and live ingestion pipelines

## Out of scope (for now)
- API ingestion code
- Production pipelines
- Live scheduling

## Structure
docs/        # Canonical schema definitions and design docs
ingestion/   # Future Python ingestion pipelines (not implemented)
analytics/   # Future factor models and analytics (not implemented)

## Status
This repository is a **draft proposal** intended to support schema design and alignment prior to implementation.
