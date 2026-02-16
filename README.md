# Trade Store Monorepo

This repository contains independently deployable services for the trade-store platform.

## Repository Layout

- `services/trade-processor`: current trade consumer/processor service
- `services/trade-ingestor`:  trade ingestion service 

## Build and Test

Run from service directory:

```powershell
cd services/trade-processor
.\mvnw.cmd test
```

## CI/CD

GitHub Actions workflow:

- `.github/workflows/deployment-pipeline.yml`

It is configured for `services/trade-processor` and includes:

- regression tests
- OSS dependency vulnerability scan (fails on high severity threshold configured in workflow)
- deploy artifact build
