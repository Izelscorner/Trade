# Production Operations Manual

This guide detail how to deploy and manage the core trading signal platform.

---

## 1. Prerequisites

### Environment Configuration
Ensure your `.env` file is populated with the following critical variables:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `DATABASE_URL` (formatted as `postgresql://user:pass@postgres:5432/db`)
- `NIM_API_KEY` (for news grading)
- `FRED_API_KEY` (for macro data)

### Network Requirements
The application requires two Docker networks (which are created automatically by Compose):
- `internal`: For service-to-service communication.
- `egress`: For fetching external data (yfinance, Google News, FRED, NIM).

---

## 2. Starting the Platform

Run the following command to build and start all production services in the background:

```bash
docker compose up -d --build
```

### Core Services Started:
- **`postgres`**: The central data store.
- **`frontend`**: React/VITE dashboard (ports `3000`).
- **`backend`**: FastAPI gateway (ports `8000`).
- **`news-fetcher`**: Constant RSS monitoring.
- **`price-fetcher`**: OHLCV data syncing.
- **`llm-processor`**: Sentiment analysis via NIM.
- **`technical-analysis` / `grading`**: Signal computation engines.

---

## 3. Monitoring & Logs

### Check Service Status
```bash
docker compose ps
```

### View Live Logs
To monitor data fetching or grading in real-time:
```bash
# All services
docker compose logs -f

# Specific service (e.g., news fetcher)
docker compose logs -f news-fetcher
```

---

## 4. Maintenance

### Database Migrations
Database tables are initialized automatically via scripts in `services/postgres/init`. If you need to reset the data:
```bash
docker compose down -v
docker compose up -d
```

### Updating Software
After making code changes (e.g., applying calibrated weights):
```bash
docker compose up -d --build
```
