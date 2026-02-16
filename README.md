# Trade Store Monorepo

This repository contains independently deployable services for the trade-store platform.

## Repository Layout

- `services/trade-processor`: current trade consumer/processor service
- `services/trade-ingestor`: trade ingestion service

## Build and Test

Run from service directory:

```powershell
cd services/trade-processor
.\mvnw.cmd test
```

## Run

### Prerequisites

- Java 21
- Kafka on `localhost:9092` (for message publish/consume flows)
- PostgreSQL on `localhost:5432` (required by `trade-processor`)
- Cassandra on `localhost:9042` (required by `trade-processor`)

### Start trade-ingestor

```powershell
cd services/trade-ingestor
.\mvnw.cmd spring-boot:run
```

Default HTTP port: `8081`

### Start trade-processor

```powershell
cd services/trade-processor
.\mvnw.cmd spring-boot:run
```

Default HTTP port: `8080`

## CI/CD

GitHub Actions workflow:

- `.github/workflows/deployment-pipeline.yml`

It is configured for `services/trade-processor` and includes:

- regression tests
- OSS dependency vulnerability scan (fails on high severity threshold configured in workflow)
- deploy artifact build
- `trade-ingestor` CI/CD mirrors the same stages in `.github/workflows/deployment-pipeline-ingestor.yml`
