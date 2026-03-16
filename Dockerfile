FROM node:25-alpine AS ui-build
WORKDIR /app

RUN (corepack --version 2>/dev/null || npm install -g --force --ignore-scripts corepack) && corepack enable

COPY frontend/package.json frontend/pnpm-lock.yaml ./frontend/
WORKDIR /app/frontend
RUN pnpm install --frozen-lockfile --ignore-scripts

COPY frontend/ /app/frontend/
RUN pnpm build

FROM python:3.14-slim AS py-build

ENV PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock README.md ./
COPY src/ ./src/
COPY *proprietary/data/seed/ ./src/snapper/data/seed/

RUN python -m pip install --upgrade pip poetry \
 && poetry config virtualenvs.create false \
 && poetry install --only=main,cloud --no-root \
 && pip wheel --wheel-dir /wheels .

FROM python:3.14-slim AS api

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SERVER_HOST=0.0.0.0

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    unixodbc \
    && rm -rf /var/lib/apt/lists/*

ARG UID=10000
RUN adduser --disabled-password --gecos '' --no-create-home --uid "$UID" snapper

COPY --from=py-build /wheels /wheels
RUN python -m pip install --upgrade pip \
 && pip install --no-index --find-links=/wheels --no-compile /wheels/*.whl

COPY --from=ui-build /app/frontend/dist ./frontend/dist

COPY alembic.ini ./
COPY src/snapper/data/migrations ./src/snapper/data/migrations
COPY *proprietary/data/migrations ./proprietary/data/migrations

RUN mkdir -p /app/data && chown snapper:snapper /app/data

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

USER snapper

EXPOSE 8000
ENTRYPOINT ["snapper"]
CMD ["server"]
