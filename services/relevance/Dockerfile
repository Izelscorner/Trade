FROM python:3.12-slim AS builder

WORKDIR /build
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

COPY --from=builder /install /usr/local
WORKDIR /app
COPY app/ ./app/

RUN mkdir -p /models && chown appuser:appgroup /models

USER appuser

EXPOSE 8002

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002", "--timeout-keep-alive", "300"]
