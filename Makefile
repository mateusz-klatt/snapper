.PHONY: help system-deps setup setup-full local-plugin mcp-pat py-refresh mcp-refresh actions-refresh refresh update pre-refresh sync-docker-tool-pins fmt fmt-fix lint lint-fix typecheck test test-serial test-integration cov cov-serial cov-xml migrate-dev-sqlite check fix check-all fix-all check-exclusions check-docstrings check-no-comments check-main-guard check-temporal-mutations check-init-files check-vendor-neutral check-pydantic-routes move-imports run-server run-static run-polygon-aggregates run-polygon-grouped migrate-dev migrate-prod dev-backend dev-notify dev-all dev-frontend run-broker run-feed run-executor run-trader-zmq zmq-logger ui-setup ui-refresh ui-dev ui-build ui-typecheck ui-lint ui-lint-fix ui-format ui-format-fix ui-dead-code ui-dead-code-fix ui-check ui-fix ui-gen-api-types ui-gen-ws-types ui-gen-zod ui-gen-api-zod ui-gen-entities ui-gen-permissions ui-gen-types ui-check-types ui-test ui-test-serial ui-cov ui-cov-serial ui-i18n-check ts-bridge bridge-regen bridge-check ios-gen-types ios-i18n-check docker-build-dev docker-build-prod docker-migrate-dev docker-migrate-prod docker-push docker-run docker-run-static docker-polygon-aggregates docker-polygon-grouped docker-stop server-check docs-pdf clean

help:
	$(info Snapper Makefile - Authoritative Development Workflow)
	$(info )
	$(info Setup:)
	$(info system-deps Install system dependencies [Linux/macOS])
	$(info setup       Create virtual environment and install dependencies)
	$(info pre-refresh Sync pre-commit hooks with pyproject versions)
	$(info py-refresh      Refresh Python dependencies [upgrade to latest])
	$(info mcp-refresh     Refresh snapper-mcp dependencies [upgrade to latest])
	$(info actions-refresh Bump GitHub Actions versions across all workflow files)
	$(info refresh         Refresh pre-commit, Python, UI, snapper-mcp deps + Actions)
	$(info update      Refresh deps, local tools, and Dockerfile tool pins)
	$(info setup-full  Install system deps + setup [Linux/macOS only])
	$(info local-plugin Render local Snapper MCP plugin into data/ + patch ~/.claude/settings.json)
	$(info mcp-pat     Mint long-lived MCP delegate JWT to data/dev-pat.json [admin creds from seed: data/ -> proprietary/ -> bundled OSS, mcp profile preferred over dev])
	$(info )
	$(info Quality Gates:)
	$(info fmt                       Check formatting [ruff, black, isort])
	$(info fmt-fix                   Auto-fix formatting [ruff, isort, black])
	$(info lint                      Run linting checks [ruff])
	$(info lint-fix                  Auto-fix linting issues [ruff --fix])
	$(info typecheck                 Run type checking [mypy])
	$(info test                      Run unit tests in parallel [pytest -n auto, isolated SQLite dev.db])
	$(info test-serial               Run unit tests sequentially [debugging, isolated SQLite dev.db])
	$(info test-integration          Run paper-mode E2E integration tests [tests/integration/])
	$(info cov                       Run tests with coverage [parallel, 100% required, isolated SQLite dev.db])
	$(info cov-serial                Run tests with coverage [sequential, isolated SQLite dev.db])
	$(info cov-xml                   Run tests with coverage + export XML [for SonarCloud])
	$(info migrate-dev-sqlite        Rebuild ./data/dev.db SQLite fixture used by test/cov)
	$(info                           override TEST_DB_URL to opt into Postgres test runs [staging only])
	$(info check                     Backend quality checks [fmt + lint + typecheck])
	$(info fix                       Backend quality fixes [fmt-fix + lint-fix + move-imports])
	$(info check-all                 Complete quality gate [check + ui + exclusions + cov])
	$(info check-exclusions          Fail if pragma/noqa/ignore comments exist [strict])
	$(info check-docstrings          Check docstring compliance [Google/BDD style])
	$(info check-no-comments         Fail if Python hash comments exist [strict])
	$(info check-main-guard          Validate __main__ blocks use raise SystemExit)
	$(info check-pydantic-routes     Fail if FastAPI routes use dict/Any/Response on I/O)
	$(info check-init-files          Validate __init__.py files are empty [strict])
	$(info check-temporal-mutations  Fail if forbidden temporal mutations exist)
	$(info fix-all                   Complete quality fixes [backend + frontend])
	$(info )
	$(info Application:)
	$(info run-server                 Start web dashboard [production mode])
	$(info run-static                 Refresh verified symbol mappings)
	$(info run-polygon-aggregates     Backfill Polygon OHLCV [settings symbols; ["*"] = all mapped])
	$(info run-polygon-grouped        Backfill Polygon grouped daily [CLI])
	$(info migrate-dev                Run migrations + seed dev data)
	$(info migrate-prod               Run migrations + seed prod data)
	$(info )
	$(info Development [hot reload]:)
	$(info dev-backend  Start backend with auto-reload [REST + WS + ZMQ broker in-process])
	$(info dev-notify   Start APNs notify sidecar [ZMQ alerts.* -> APNs HTTP/2])
	$(info dev-all      Start backend + notify sidecar in parallel [Ctrl-C stops both])
	$(info dev-frontend Start frontend dev server [Vite with HMR + proxy])
	$(info )
	$(info ZeroMQ IPC:)
	$(info run-broker     Start ZMQ broker for message routing)
	$(info run-feed       Start market data feed publisher)
	$(info run-executor   Start execution service)
	$(info run-trader-zmq Start ZMQ-enabled trader [paper mode])
	$(info zmq-logger     Start ZMQ message logger [debug tool])
	$(info )
	$(info UI [pnpm]:)
	$(info ui-setup           Install UI dependencies with pnpm)
	$(info ui-refresh         Clean install UI dependencies)
	$(info ui-dev             Start Vite dev server)
	$(info ui-build           Build UI for production)
	$(info ui-lint            Lint UI code)
	$(info ui-lint-fix        Auto-fix UI linting errors)
	$(info ui-format          Check UI code formatting [CI-style])
	$(info ui-format-fix      Format UI code with Prettier)
	$(info ui-dead-code       Check for dead code in UI)
	$(info ui-dead-code-fix   Auto-fix dead code in UI)
	$(info ui-gen-api-types   Generate TypeScript types from OpenAPI)
	$(info ui-gen-ws-types    Generate TypeScript types from WebSocket)
	$(info ui-gen-zod         Generate Zod schemas from WebSocket)
	$(info ui-gen-api-zod     Generate Zod schemas from OpenAPI)
	$(info ui-gen-entities    Generate entity types from WebSocket)
	$(info ui-gen-permissions Generate permissions types from backend)
	$(info ui-gen-types       Generate all frontend types)
	$(info ui-check-types     Check for uncommitted type drift [CI])
	$(info ui-test            Run UI tests [vitest])
	$(info ui-test-serial     Run UI tests sequentially [debugging])
	$(info ui-cov             Run UI tests with coverage)
	$(info ui-cov-serial      Run UI tests with coverage sequentially [debugging])
	$(info ui-i18n-check      Check for hardcoded user-facing strings in frontend)
	$(info ui-check           UI quality checks [lint + format + dead-code + i18n])
	$(info ui-fix             UI quality fixes)
	$(info )
	$(info iOS [snapper-ios submodule]:)
	$(info ios-gen-types Generate Swift types from OpenAPI + WebSocket [writes into submodule])
	$(info Other iOS targets live in the snapper-ios submodule \(cd ios && make ...\))
	$(info )
	$(info Docker:)
	$(info docker-build-dev              Build Docker image [caller UID])
	$(info docker-build-prod             Build Docker image [UID 888])
	$(info docker-migrate-dev            Run migrations + seed dev data in Docker)
	$(info docker-migrate-prod           Run migrations + seed prod data in Docker)
	$(info docker-push                   Push Docker image)
	$(info docker-run                    Run Docker container [background])
	$(info docker-run-static             Refresh verified symbol mappings in Docker)
	$(info docker-polygon-aggregates     Backfill Polygon OHLCV in Docker [settings symbols; ["*"] = all mapped])
	$(info docker-polygon-grouped        Backfill Polygon grouped daily in Docker)
	$(info docker-stop                   Stop Docker container)
	$(info server-check                  Health check server [cross-platform])
	$(info )
	$(info Docs:)
	$(info docs-pdf Export README + docs/*.md into frontend/public/snapper.pdf)
	$(info )
	$(info Code Maintenance:)
	$(info move-imports Move all imports to top of Python files)
	$(info )
	$(info Cleanup:)
	$(info clean Remove build artifacts and caches [cross-platform])
	@:

ifeq ($(OS),Windows_NT)
  PYTHON   := python
  VENV_PY  := .venv\Scripts\python
  DEVNULL  := NUL
else
  DEVNULL  := /dev/null
  UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Darwin)
    PYTHON := $(shell which python3.14 2>/dev/null || which python3)
  else
    PYTHON := $(shell which python3.14 2>/dev/null || which python3)
  endif
  VENV_PY  := .venv/bin/python
endif

DOCKER_NAME := snapper
IMAGE_NAME := klattm/snapper
IMAGE_TAG := latest
PYRUN := $(VENV_PY) -m
PYTEST_TIMEOUT := --timeout=15 --timeout-method=thread
ROOT_DIR := $(CURDIR)
SERVER_PORT ?= 8000
UI_DIR := frontend

DOCKER_BASE := docker run --env-file "$(CURDIR)/.env" -v "$(CURDIR)/data":/app/data
DOCKER_RUN := $(DOCKER_BASE) --rm $(IMAGE_NAME):$(IMAGE_TAG)
GENSCRIPT := @$(VENV_PY) scripts/generate_types.py
PNPM := @cd $(UI_DIR) && pnpm
PRETTIER := $(PNPM) exec prettier --write
PYTEST_PARALLEL := -n $(shell $(PYTHON) -c "import math,os; print(min(6, max(1, math.ceil((os.cpu_count() or 1)/2))))")
PY_DIRS := src tests scripts $(wildcard proprietary/src) $(wildcard proprietary/tests)

system-deps:
ifeq ($(OS),Windows_NT)
	$(info On Windows, please manually install:)
	$(info - Microsoft C++ Build Tools or Visual Studio)
	$(info - curl [usually available in Windows 10+])
else ifeq ($(shell uname),Darwin)
	@echo "Detected macOS"
else ifeq ($(shell command -v apt-get 2>/dev/null),)
	@echo "Detected RHEL/CentOS - using yum"
	@sudo yum groupinstall -y "Development Tools"
	@sudo yum install -y curl
else
	@echo "Detected Ubuntu/Debian - using apt-get"
	@sudo apt-get update
	@sudo apt-get install -y --no-install-recommends build-essential curl
endif
	@(corepack --version 2>/dev/null && echo "corepack already installed") || (echo "Installing corepack..." && sudo npm install -g --ignore-scripts corepack)
	@corepack enable

setup:
	$(info Setting up development environment...)
	$(PYTHON) -m venv .venv
	$(PYRUN) pip install --upgrade pip
	$(PYRUN) pip install --upgrade poetry
	$(PYRUN) poetry install --with dev --with cloud
	$(PYRUN) pre_commit install
	$(info Setup completed!)

setup-full: system-deps setup

local-plugin:
	$(info Rendering local Snapper MCP plugin into data/snapper-mcp-local-plugin/...)
	$(VENV_PY) scripts/render_local_plugin.py

mcp-pat:
	$(info Minting MCP access token (admin creds from seed: data/ -> proprietary/ -> bundled OSS; mcp profile -> dev fallback)...)
	$(PYRUN) snapper dev-mint-pat

py-refresh:
	$(info Upgrading local Poetry tool...)
	$(PYRUN) pip install --upgrade poetry
	$(info Clearing Poetry cache...)
	-$(PYRUN) poetry cache clear --all -n .
	$(info Bumping Python dependency constraints to latest available versions...)
	-$(PYRUN) poetry up --latest
	$(info Refreshing Python lock file within the current constraints...)
	$(PYRUN) poetry update
	$(info Python dependencies refreshed!)

mcp-refresh:
	$(info Refreshing snapper-mcp dependencies...)
	$(VENV_PY) -m scripts.mcp_refresh

actions-refresh:
	$(info Refreshing GitHub Actions versions across parent + submodule workflows...)
	$(VENV_PY) -m scripts.refresh_github_actions

refresh: py-refresh ui-refresh mcp-refresh actions-refresh pre-refresh
	$(info All dependencies refreshed!)

sync-docker-tool-pins:
	$(VENV_PY) scripts/update_tool_pins.py

update: refresh sync-docker-tool-pins
	$(info All dependencies and Docker build-tool pins refreshed!)

pre-refresh:
	$(info Synchronizing pre-commit hook versions from pyproject.toml...)
	$(VENV_PY) scripts/sync_precommit.py
	$(info Pre-commit hooks refreshed!)

fmt:
	$(PYRUN) ruff check $(PY_DIRS)
	$(PYRUN) black --check $(PY_DIRS)
	$(PYRUN) isort --check-only $(PY_DIRS)

fmt-fix:
	$(PYRUN) ruff check --fix-only $(PY_DIRS)
	$(PYRUN) isort $(PY_DIRS)
	$(PYRUN) black $(PY_DIRS)

lint:
	$(PYRUN) ruff check $(PY_DIRS)

lint-fix:
	$(PYRUN) ruff check --fix $(PY_DIRS)

typecheck:
	$(PYRUN) mypy $(PY_DIRS)

TEST_DB_FILE := ./data/dev.db
TEST_DB_URL ?= sqlite+aiosqlite:///$(TEST_DB_FILE)

ifeq ($(OS),Windows_NT)
  WITH_TEST_DB := set DB_URL=$(TEST_DB_URL)&&
else
  WITH_TEST_DB := DB_URL="$(TEST_DB_URL)"
endif

$(TEST_DB_FILE):
	@echo "Bootstrapping local SQLite test fixture at $(TEST_DB_FILE)..."
	$(WITH_TEST_DB) $(PYRUN) snapper db-init
	$(WITH_TEST_DB) $(PYRUN) snapper db-seed --profile dev

migrate-dev-sqlite: $(TEST_DB_FILE)

test: $(TEST_DB_FILE)
	$(WITH_TEST_DB) $(PYRUN) pytest $(PYTEST_PARALLEL) $(PYTEST_TIMEOUT) --max-worker-restart=0

test-serial: $(TEST_DB_FILE)
	$(WITH_TEST_DB) $(PYRUN) pytest $(PYTEST_TIMEOUT)

test-integration: $(TEST_DB_FILE)
	$(WITH_TEST_DB) $(PYRUN) pytest tests/integration/ -v -m integration --timeout=120

cov: $(TEST_DB_FILE)
	$(WITH_TEST_DB) $(PYRUN) pytest $(PYTEST_PARALLEL) --cov $(PYTEST_TIMEOUT) --max-worker-restart=0

cov-serial: $(TEST_DB_FILE)
	$(WITH_TEST_DB) $(PYRUN) pytest --cov $(PYTEST_TIMEOUT)

cov-xml:
	$(PYRUN) coverage xml -o coverage.xml

check: fmt lint typecheck check-docstrings check-no-comments check-main-guard check-init-files check-temporal-mutations check-vendor-neutral check-pydantic-routes

fix: fmt-fix lint-fix move-imports

check-all: check ui-check check-exclusions cov ui-cov
	$(info All quality checks passed [backend + frontend + 100% coverage TDD])

fix-all: fix ui-fix
	$(info All quality fixes applied [backend + frontend])

check-exclusions:
	$(VENV_PY) scripts/check_coverage_exclusions.py --strict

check-docstrings:
	$(VENV_PY) scripts/check_docstrings.py --strict --verbose --enforce-bdd --enforce-google-sections

check-no-comments:
	$(VENV_PY) scripts/check_no_comments.py --strict

check-main-guard:
	$(VENV_PY) scripts/check_main_guard.py --strict

check-init-files:
	$(VENV_PY) scripts/check_init_files.py --strict

check-temporal-mutations:
	$(VENV_PY) scripts/check_temporal_mutations.py --strict

check-vendor-neutral:
	$(VENV_PY) scripts/check_vendor_neutral.py --strict

check-pydantic-routes:
	$(VENV_PY) scripts/check_pydantic_routes.py --strict

move-imports:
	$(VENV_PY) scripts/move_imports_to_top.py $(PY_DIRS)
	$(info Imports moved to top of files)

run-server:
	$(PYRUN) snapper server --host 0.0.0.0

dev-backend:
	$(info Starting backend with hot reload...)
	$(info Backend API: http://localhost:8000/api)
	$(info WebSocket: ws://localhost:8000/api/ws)
	$(info Backend log: data/snapper.log)
	@mkdir -p data
	@bash -c '$(PYRUN) snapper server --host 0.0.0.0 --reload 2>&1 | sed "s/\x1b\[[0-9;]*m//g" | tee data/snapper.log'

dev-notify:
	$(info Starting iOS Push Foundation sidecar (ZMQ alerts -> APNs)...)
	$(info Topic + APNs creds read from settings cache (apns_*).)
	$(info Sidecar log: data/snapper-notify.log)
	@mkdir -p data
	@bash -c '$(PYRUN) snapper notify 2>&1 | sed "s/\x1b\[[0-9;]*m//g" | tee data/snapper-notify.log'

dev-all:
	$(info Starting backend + notify sidecar in parallel...)
	$(info Backend API: http://localhost:8000/api)
	$(info WebSocket: ws://localhost:8000/api/ws)
	$(info Notify sidecar fans out alerts.* -> APNs.)
	$(info Logs: data/snapper.log + data/snapper-notify.log)
	$(info Press Ctrl-C to stop both processes.)
	@mkdir -p data
	@bash -c 'set -m; trap "kill 0 2>/dev/null; exit" SIGINT SIGTERM EXIT; \
		($(PYRUN) snapper server --host 0.0.0.0 --reload 2>&1 \
			| sed "s/\x1b\[[0-9;]*m//g" \
			| awk "{print \"[backend] \" \$$0; fflush()}" \
			| tee data/snapper.log) & \
		($(PYRUN) snapper notify 2>&1 \
			| sed "s/\x1b\[[0-9;]*m//g" \
			| awk "{print \"[notify]  \" \$$0; fflush()}" \
			| tee data/snapper-notify.log) & \
		wait'

dev-frontend:
	$(info Starting frontend dev server...)
	$(info Frontend URL: http://localhost:3000/)
	$(info API proxy: http://localhost:3000/api -> http://localhost:8000/api)
	$(PNPM) dev --host 0.0.0.0

run-static:
	$(PYRUN) snapper update-kraken-symbols --force
	$(PYRUN) snapper update-kraken-futures-symbols --force
	$(PYRUN) snapper update-kraken-equities-symbols --force
	$(PYRUN) snapper update-walutomat-symbols --force
	$(PYRUN) snapper update-polygon-symbols --force || true
	$(PYRUN) snapper update-underlyings
	$(PYRUN) snapper update-kraken-market-snapshot
	$(PYRUN) snapper update-kraken-futures-market-snapshot
	$(PYRUN) snapper update-kraken-equities-market-snapshot
	$(PYRUN) snapper update-walutomat-market-snapshot

run-polygon-aggregates:
	$(PYRUN) snapper polygon-backfill-aggregates -d 32
	$(PYRUN) snapper polygon-backfill-aggregates -d 730

run-polygon-grouped:
	$(PYRUN) snapper polygon-backfill-grouped -m crypto -d 729
	$(PYRUN) snapper polygon-backfill-grouped -m stocks -l us -d 729
	$(PYRUN) snapper polygon-backfill-grouped -m fx -d 729

backfill-kraken-equities-candles:
	$(PYRUN) snapper kraken-equities-backfill-candles -t 1h -d 30

run-broker:
	$(PYRUN) snapper broker

run-feed:
	$(PYRUN) snapper feed --symbols BTC/USD,ETH/USD --through-broker true

run-executor:
	$(PYRUN) snapper executor

run-trader-zmq:
	$(PYRUN) snapper trade-zmq --strategy rsi_reversion --paper

zmq-logger:
	$(PYRUN) snapper zmq-logger --payload --max-length 500

migrate-dev:
	$(PYRUN) snapper db-init
	$(PYRUN) snapper db-seed --profile dev

migrate-prod:
	$(PYRUN) snapper db-init
	$(PYRUN) snapper db-seed --profile prod

ui-setup:
	@corepack --version >$(DEVNULL) 2>&1 || (echo "Error: corepack not found. Run 'make system-deps' first." && exit 1)
	$(PNPM) install --frozen-lockfile

ui-refresh:
	$(PYTHON) scripts/ui_refresh.py

ui-dev:
	$(PNPM) dev

ui-build:
	$(PNPM) build

ui-typecheck:
	$(PNPM) typecheck

ui-lint:
	$(PNPM) lint

ui-lint-fix:
	$(PNPM) lint:fix

ui-format:
	$(PNPM) format:check

ui-format-fix:
	$(PNPM) format

ui-i18n-check:
	$(PNPM) check:i18n

ui-i18n-check-alerts:
	$(VENV_PY) scripts/port_ios_alert_catalog.py --check

ui-check: ui-lint ui-format ui-dead-code ui-typecheck ui-i18n-check ui-i18n-check-alerts
	$(info UI quality checks passed [lint + format + dead code + typecheck + i18n + alerts])

ui-fix: ui-lint-fix ui-format-fix ui-dead-code-fix
	$(info UI quality fixes applied [lint + format + dead code])

ui-dead-code:
	$(PNPM) dead-code

ui-dead-code-fix:
	$(PNPM) dead-code:fix

ui-gen-api-types:
	$(info Generating TypeScript types from OpenAPI schema...)
	$(info Exporting OpenAPI schema from FastAPI...)
	$(GENSCRIPT) --openapi
	$(PNPM) gen:api-types:from-monorepo
	$(GENSCRIPT) --postprocess-openapi-types
	$(PRETTIER) src/types/api.generated.ts
	$(info Generated frontend/src/types/api.generated.ts)

ui-gen-ws-types:
	$(info Generating TypeScript types from WebSocket schemas...)
	$(GENSCRIPT) --export
	$(PNPM) gen:ws-types:from-monorepo
	$(GENSCRIPT) --strip-eslint-disable
	$(PRETTIER) src/types/ws.generated.ts
	$(info Generated frontend/src/types/ws.generated.ts)

ui-gen-zod:
	$(info Generating Zod schemas from WebSocket JSON Schema...)
	$(GENSCRIPT) --frontend-ws
	$(PRETTIER) src/lib/schemas/ws.generated.zod.ts
	$(info Generated frontend/src/lib/schemas/ws.generated.zod.ts)

ui-gen-api-zod:
	$(info Generating Zod schemas from OpenAPI schema...)
	$(GENSCRIPT) --frontend-api
	$(PRETTIER) src/lib/schemas/api.generated.zod.ts
	$(info Generated frontend/src/lib/schemas/api.generated.zod.ts)

ui-gen-entities:
	$(info Generating entity types...)
	$(GENSCRIPT) --entities
	$(PRETTIER) src/types/entities.generated.ts
	$(info Generated frontend/src/types/entities.generated.ts)

ui-gen-permissions:
	$(info Generating permissions types...)
	$(GENSCRIPT) --permissions
	$(PRETTIER) src/types/permissions.generated.ts
	$(info Generated frontend/src/types/permissions.generated.ts)

ui-gen-types: ui-gen-api-types ui-gen-ws-types ui-gen-zod ui-gen-api-zod ui-gen-entities ui-gen-permissions
	$(info All types generated successfully)

ui-check-types:
	$(info Checking for type drift [non-destructive]...)
	@$(VENV_PY) scripts/check_type_drift.py

ui-test:
	$(PNPM) test:run

ui-test-serial:
	$(PNPM) test:run:serial

ui-cov:
	$(PNPM) test:coverage

ui-cov-serial:
	$(PNPM) test:coverage:serial

ts-bridge:
	$(info Regenerating bridge wire-contract types into the working tree...)
	$(GENSCRIPT) --bridge
	$(info Generated integrations/snapper-mcp/src/generated/wire-contract.ts)

bridge-regen: ts-bridge
	$(info Running bridge stack against freshly-regenerated working tree...)
	cd integrations/snapper-mcp && npm ci && npm run typecheck && npm run lint && npm run test && npm run stdout-gate

bridge-check:
	$(info Verifying bridge wire-contract working-tree file is consistent with current backend schemas...)
	@$(VENV_PY) -m scripts.bridge_check.check_drift
	@$(VENV_PY) -m scripts.bridge_check.check_oss_prose
	cd integrations/snapper-mcp && npm ci && npm run typecheck && npm run lint && npm run test && npm run stdout-gate

docs-pdf:
	$(VENV_PY) scripts/build_docs_pdf.py
	$(info Generated frontend/public/snapper.pdf)

ios-gen-types:
	$(info Generating Swift types from backend schemas...)
	$(GENSCRIPT) --openapi --export --ios
	$(info Generated iOS types in ios/Snapper/Models/Generated/)
	$(info Commit + push from inside the ios submodule, then bump the parent pointer.)

gen-backend-i18n-catalog:
	$(info Generating backend i18n catalog JSONs from iOS xcstrings...)
	$(VENV_PY) scripts/gen_backend_i18n_catalog.py
	$(info Generated backend catalogs in src/snapper/i18n/catalogs/)

ios-i18n-check:
	$(MAKE) -C ios ios-i18n-check

DOCKER_PROD_UID := 888
ifeq ($(OS),Windows_NT)
  DOCKER_DEV_UID := $(DOCKER_PROD_UID)
else
  DOCKER_DEV_UID := $(shell id -u)
  ifeq ($(DOCKER_DEV_UID),0)
    DOCKER_DEV_UID := $(DOCKER_PROD_UID)
  endif
endif

docker-build-dev:
	docker build --build-arg UID=$(DOCKER_DEV_UID) -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-build-prod:
	docker build --build-arg UID=$(DOCKER_PROD_UID) -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-migrate-dev:
	$(DOCKER_RUN) db-init
	$(DOCKER_RUN) db-seed --profile dev

docker-migrate-prod:
	$(DOCKER_RUN) db-init
	$(DOCKER_RUN) db-seed --profile prod

docker-push:
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

docker-run:
	$(DOCKER_BASE) -d --rm --name $(DOCKER_NAME) -p 127.0.0.1:$(SERVER_PORT):$(SERVER_PORT) $(IMAGE_NAME):$(IMAGE_TAG) server

docker-run-static:
	$(DOCKER_RUN) update-kraken-symbols --force
	$(DOCKER_RUN) update-kraken-futures-symbols --force
	$(DOCKER_RUN) update-kraken-equities-symbols --force
	$(DOCKER_RUN) update-walutomat-symbols --force
	$(DOCKER_RUN) update-polygon-symbols --force || true
	$(DOCKER_RUN) update-underlyings
	$(DOCKER_RUN) update-kraken-market-snapshot
	$(DOCKER_RUN) update-kraken-futures-market-snapshot
	$(DOCKER_RUN) update-kraken-equities-market-snapshot
	$(DOCKER_RUN) update-walutomat-market-snapshot

docker-polygon-aggregates:
	$(DOCKER_RUN) polygon-backfill-aggregates -d 32
	$(DOCKER_RUN) polygon-backfill-aggregates -d 730

docker-polygon-grouped:
	$(DOCKER_RUN) polygon-backfill-grouped -m crypto -d 729
	$(DOCKER_RUN) polygon-backfill-grouped -m stocks -l us -d 729
	$(DOCKER_RUN) polygon-backfill-grouped -m fx -d 729

docker-stop:
	-docker stop $(DOCKER_NAME)
	$(info Container stopped)

server-check:
	@$(VENV_PY) scripts/server_check.py

clean:
	$(PYTHON) scripts/clean.py
