ARG CADDY_TAG=2-alpine
FROM caddy:${CADDY_TAG} AS caddy-source

FROM node:26-alpine AS ui-build
ARG COREPACK_VERSION=0.34.0
WORKDIR /app

RUN (corepack --version 2>/dev/null || \
        npm install -g --force --ignore-scripts "corepack@${COREPACK_VERSION}") \
    && corepack enable

COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml frontend/.npmrc ./frontend/
WORKDIR /app/frontend
RUN pnpm install --frozen-lockfile --ignore-scripts

COPY frontend/ /app/frontend/
RUN pnpm build

FROM python:3.14-slim AS py-build
ARG POETRY_VERSION=2.3.4

ENV PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock README.md ./
COPY src/ ./src/
COPY *proprietary/data/seed/ ./src/snapper/data/seed/

RUN python -m venv /opt/poetry \
 && /opt/poetry/bin/pip install --only-binary :all: "poetry==${POETRY_VERSION}" \
 && python -m venv /opt/appenv \
 && /opt/poetry/bin/poetry config virtualenvs.create false \
 && /opt/poetry/bin/poetry config installer.only-binary :all: \
 && VIRTUAL_ENV=/opt/appenv PATH="/opt/appenv/bin:$PATH" /opt/poetry/bin/poetry install --only=main,cloud --no-root \
 && VIRTUAL_ENV=/opt/appenv PATH="/opt/appenv/bin:$PATH" /opt/appenv/bin/pip wheel --no-deps --wheel-dir /wheels . \
 && VIRTUAL_ENV=/opt/appenv PATH="/opt/appenv/bin:$PATH" /opt/appenv/bin/pip install \
    --no-index \
    --find-links=/wheels \
    --only-binary :all: \
    --no-deps \
    --no-compile \
    snapper==0.1.0

FROM python:3.14-slim AS runtime
LABEL org.opencontainers.image.title="snapper"
LABEL org.opencontainers.image.description="snapper — market data + execution platform. Modes selected at runtime via the snapper CLI: 'snapper server' (monolith) or 'snapper egress' (sidecar)."

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SERVER_HOST=0.0.0.0 \
    PATH=/opt/appenv/bin:$PATH

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    iproute2 \
    unixodbc \
    wireguard-tools \
    && rm -rf /var/lib/apt/lists/*

ARG UID=888
RUN adduser --disabled-password --gecos '' --no-create-home --uid "$UID" snapper

COPY --from=py-build /opt/appenv /opt/appenv

COPY --from=ui-build /app/frontend/dist ./frontend/dist
COPY --from=ui-build /app/frontend/dist /srv/dist

COPY --from=caddy-source /usr/bin/caddy /usr/bin/caddy
COPY docker/web/Caddyfile /etc/caddy/Caddyfile

COPY alembic.ini ./
COPY src/snapper/data/migrations ./src/snapper/data/migrations
COPY *proprietary/data/migrations ./proprietary/data/migrations

RUN mkdir -p /app/data && chown snapper:snapper /app/data

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

USER snapper

EXPOSE 8000 8081
ENTRYPOINT ["snapper"]
CMD ["server"]
