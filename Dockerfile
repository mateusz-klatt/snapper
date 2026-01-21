FROM node:22-alpine AS ui-build
WORKDIR /app

RUN corepack enable && corepack prepare pnpm@latest --activate

COPY frontend/package.json frontend/pnpm-lock.yaml ./frontend/
WORKDIR /app/frontend
RUN pnpm install --frozen-lockfile

COPY frontend/ /app/frontend/
RUN pnpm build

FROM python:3.12.12-slim AS py-build

ENV PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock README.md ./
COPY src/ ./src/

RUN python -m pip install --upgrade pip poetry \
 && poetry config virtualenvs.create false \
 && poetry install --only=main,cloud --no-root \
 && pip wheel --wheel-dir /wheels .

FROM python:3.12.12-slim AS api

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SERVER_HOST=0.0.0.0

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unixodbc \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=py-build /wheels /wheels
RUN python -m pip install --upgrade pip \
 && pip install --no-index --find-links=/wheels --no-compile /wheels/*.whl

COPY --from=ui-build /app/frontend/dist ./frontend/dist

COPY alembic.ini ./
COPY src/snapper/data/migrations ./src/snapper/data/migrations
COPY *proprietary/data/migrations ./proprietary/data/migrations

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/snapper/api/health || exit 1

EXPOSE 8000
ENTRYPOINT ["snapper"]
CMD ["server"]
