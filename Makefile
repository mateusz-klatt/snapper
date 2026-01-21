.PHONY: help system-deps setup setup-full py-refresh refresh pre-refresh fmt fmt-fix lint lint-fix typecheck test test-serial cov cov-serial check fix check-all fix-all check-exclusions check-docstrings check-no-comments move-imports run-collector run-trader run-paper run-backtest run-server run-static run-polygon-aggregates run-polygon-aggregates-all run-polygon-grouped migrate dev-backend dev-frontend run-broker run-feed run-executor run-trader-zmq zmq-logger ui-setup ui-refresh ui-dev ui-build ui-typecheck ui-lint ui-lint-fix ui-format ui-format-fix ui-dead-code ui-dead-code-fix ui-check ui-fix ui-gen-api-types ui-gen-ws-types ui-gen-zod ui-gen-api-zod ui-gen-entities ui-gen-types ui-check-types ui-test ui-cov ios-setup ios-gen-types ios-build ios-test ios-clean docker-build docker-migrate docker-push docker-run docker-stop server-check docs-pdf clean

help:
	$(info Snapper Makefile - Authoritative Development Workflow)
	$(info )
	$(info Setup:)
	$(info system-deps Install system dependencies [Linux/macOS])
	$(info setup       Create virtual environment and install dependencies)
	$(info pre-refresh Sync pre-commit hooks with pyproject versions)
	$(info py-refresh  Refresh Python dependencies [upgrade to latest])
	$(info refresh     Refresh pre-commit, Python, and UI dependencies)
	$(info setup-full  Install system deps + setup [Linux/macOS only])
	$(info )
	$(info Quality Gates:)
	$(info fmt               Check formatting [ruff, black, isort])
	$(info fmt-fix           Auto-fix formatting [ruff, isort, black])
	$(info lint              Run linting checks [ruff])
	$(info lint-fix          Auto-fix linting issues [ruff --fix])
	$(info typecheck         Run type checking [mypy])
	$(info test              Run unit tests in parallel [pytest -n auto])
	$(info test-serial       Run unit tests sequentially [debugging])
	$(info cov               Run tests with coverage [parallel, 100% required])
	$(info cov-serial        Run tests with coverage [sequential])
	$(info check             Backend quality checks [fmt + lint + typecheck])
	$(info fix               Backend quality fixes [fmt-fix + lint-fix + move-imports])
	$(info check-all         Complete quality gate [check + ui + exclusions + cov])
	$(info check-exclusions  Fail if pragma/noqa/ignore comments exist [strict])
	$(info check-docstrings  Check docstring compliance [Google/BDD style])
	$(info check-no-comments Fail if Python hash comments exist [strict])
	$(info fix-all           Complete quality fixes [backend + frontend])
	$(info )
	$(info Application:)
	$(info run-collector              Start data collector)
	$(info run-trader                 Start live trading)
	$(info run-paper                  Start paper trading [no real orders])
	$(info run-backtest               Run strategy backtest)
	$(info run-server                 Start web dashboard [production mode])
	$(info run-static                 Refresh verified symbol mappings)
	$(info run-polygon-aggregates     Backfill Polygon OHLCV [settings symbols])
	$(info run-polygon-aggregates-all Backfill Polygon OHLCV [all mapped symbols])
	$(info run-polygon-grouped        Backfill Polygon grouped daily [CLI])
	$(info migrate                    Run database migrations)
	$(info )
	$(info Development [hot reload]:)
	$(info dev-backend  Start backend with auto-reload)
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
	$(info ui-setup         Install UI dependencies with pnpm)
	$(info ui-refresh       Clean install UI dependencies)
	$(info ui-dev           Start Vite dev server)
	$(info ui-build         Build UI for production)
	$(info ui-lint          Lint UI code)
	$(info ui-lint-fix      Auto-fix UI linting errors)
	$(info ui-format        Check UI code formatting [CI-style])
	$(info ui-format-fix    Format UI code with Prettier)
	$(info ui-dead-code     Check for dead code in UI)
	$(info ui-dead-code-fix Auto-fix dead code in UI)
	$(info ui-gen-api-types Generate TypeScript types from OpenAPI)
	$(info ui-gen-ws-types  Generate TypeScript types from WebSocket)
	$(info ui-gen-zod       Generate Zod schemas from WebSocket)
	$(info ui-gen-api-zod   Generate Zod schemas from OpenAPI)
	$(info ui-gen-entities  Generate entity types from WebSocket)
	$(info ui-gen-types     Generate all frontend types)
	$(info ui-check-types   Check for uncommitted type drift [CI])
	$(info ui-test          Run UI tests [vitest])
	$(info ui-cov           Run UI tests with coverage)
	$(info ui-check         UI quality checks [lint + format + dead-code])
	$(info ui-fix           UI quality fixes)
	$(info )
	$(info iOS [Xcode]:)
	$(info ios-setup     Setup iOS project [xcodegen + test target])
	$(info ios-gen-types Generate Swift types from OpenAPI + WebSocket)
	$(info ios-build     Build iOS app)
	$(info ios-test      Run iOS unit tests)
	$(info ios-clean     Clean iOS build artifacts)
	$(info )
	$(info Docker:)
	$(info docker-build   Build Docker image)
	$(info docker-migrate Run database migrations in Docker)
	$(info docker-push    Push Docker image)
	$(info docker-run     Run Docker container [background])
	$(info docker-stop    Stop Docker container)
	$(info server-check   Health check server [cross-platform])
	$(info )
	$(info Docs:)
	$(info docs-pdf Export README + docs/*.md into snapper.pdf)
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
else
  UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Darwin)
    PYTHON := $(shell which python3.12 2>/dev/null || which python3)
  else
    PYTHON := python3
  endif
  VENV_PY  := .venv/bin/python
endif

PYRUN := $(VENV_PY) -m
IMAGE_NAME := klattm/snapper
IMAGE_TAG := latest
UI_DIR := frontend
ROOT_DIR := $(CURDIR)

system-deps:
ifeq ($(OS),Windows_NT)
	$(info On Windows, please manually install:)
	$(info - Microsoft C++ Build Tools or Visual Studio)
	$(info - ODBC Driver for SQL Server [if using Azure SQL])
	$(info - curl [usually available in Windows 10+])
else ifeq ($(shell uname),Darwin)
	@echo "Detected macOS - installing unixodbc via brew"
	@brew install unixodbc || true
else ifeq ($(shell command -v apt-get 2>/dev/null),)
	@echo "Detected RHEL/CentOS - using yum"
	@sudo yum groupinstall -y "Development Tools"
	@sudo yum install -y curl unixODBC-devel
else
	@echo "Detected Ubuntu/Debian - using apt-get"
	@sudo apt-get update
	@sudo apt-get install -y --no-install-recommends build-essential curl unixodbc-dev
endif

setup:
	$(info Setting up development environment...)
	$(PYTHON) -m venv .venv
	$(PYRUN) pip install --upgrade pip
	$(PYRUN) pip install --upgrade poetry
	$(PYRUN) poetry install --with dev --with cloud
	$(PYRUN) pre_commit install
	$(info Setup completed!)

setup-full: system-deps setup

py-refresh:
	$(info Clearing Poetry cache...)
	-$(PYRUN) poetry cache clear --all -n .
	$(info Refreshing Python dependencies...)
	$(PYRUN) poetry up --latest || $(PYRUN) poetry update
	$(info Python dependencies refreshed!)

refresh: py-refresh ui-refresh pre-refresh
	$(info All dependencies refreshed!)

pre-refresh:
	$(info Synchronizing pre-commit hook versions from pyproject.toml...)
	$(VENV_PY) scripts/sync_precommit.py
	$(info Pre-commit hooks refreshed!)

PY_DIRS := src tests scripts $(wildcard proprietary/src) $(wildcard proprietary/tests)

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

PYTEST_PARALLEL := -n $(shell $(PYTHON) -c "import os,math; print(math.ceil(os.cpu_count()/2))")

test:
	$(PYRUN) pytest $(PYTEST_PARALLEL) --timeout=15 --timeout-method=thread

test-serial:
	$(PYRUN) pytest --timeout=15 --timeout-method=thread

cov:
	$(PYRUN) pytest $(PYTEST_PARALLEL) --cov --timeout=15 --timeout-method=thread

cov-serial:
	$(PYRUN) pytest --cov --timeout=15 --timeout-method=thread

check: fmt lint typecheck check-docstrings check-no-comments

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

move-imports:
	$(VENV_PY) scripts/move_imports_to_top.py $(PY_DIRS)
	$(info Imports moved to top of files)

run-collector:
	$(PYRUN) snapper collect

run-trader:
	$(PYRUN) snapper trade --strategy rsi_reversion

run-paper:
	$(PYRUN) snapper trade --strategy rsi_reversion --paper

run-backtest:
	$(PYRUN) snapper backtest --strategy macd_crossover --from 2024-01-01 --to 2024-12-31

run-server:
	$(PYRUN) snapper server

dev-backend:
	$(info Starting backend with hot reload...)
	$(info Backend API: http://localhost:8000/snapper/api)
	$(info WebSocket: ws://localhost:8000/snapper/api/ws)
	@bash -c 'script -q -e -c "$(PYRUN) snapper server --reload" /dev/null 2>&1 | tee >(sed "s/\x1b\[[0-9;]*m//g" > data/snapper.log)'

dev-frontend:
	$(info Starting frontend dev server...)
	$(info Frontend URL: http://localhost:3000/snapper/)
	$(info API proxy: http://localhost:3000/snapper/api -> http://localhost:8000/snapper/api)
	@cd $(UI_DIR) && pnpm dev

run-static:
	$(PYRUN) snapper update-kraken-symbols --force
	$(PYRUN) snapper update-zonda-symbols --force
	$(PYRUN) snapper update-walutomat-symbols --force
	$(PYRUN) snapper update-polygon-symbols --force
	$(PYRUN) snapper update-kraken-market-snapshot
	$(PYRUN) snapper update-zonda-market-snapshot
	$(PYRUN) snapper update-walutomat-market-snapshot

run-polygon-aggregates:
	$(PYRUN) snapper polygon-backfill-aggregates -d 32
	$(PYRUN) snapper polygon-backfill-aggregates -d 730

run-polygon-aggregates-all:
	$(PYRUN) snapper polygon-backfill-aggregates --all -d 32
	$(PYRUN) snapper polygon-backfill-aggregates --all -d 730

run-polygon-grouped:
	$(PYRUN) snapper polygon-backfill-grouped -m crypto -d 729
	$(PYRUN) snapper polygon-backfill-grouped -m stocks -l us -d 729
	$(PYRUN) snapper polygon-backfill-grouped -m fx -d 729

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

migrate:
	$(PYRUN) snapper db-init

ui-setup:
	@cd $(UI_DIR) && (pnpm --version 2>&1 || (corepack enable && corepack prepare pnpm@latest --activate)) && pnpm install --frozen-lockfile

ui-refresh:
	$(PYTHON) scripts/ui_refresh.py

ui-dev:
	@cd $(UI_DIR) && pnpm dev

ui-build:
	@cd $(UI_DIR) && pnpm build

ui-typecheck:
	@cd $(UI_DIR) && pnpm typecheck

ui-lint:
	@cd $(UI_DIR) && pnpm lint

ui-lint-fix:
	@cd $(UI_DIR) && pnpm lint:fix

ui-format:
	@cd $(UI_DIR) && pnpm format:check

ui-format-fix:
	@cd $(UI_DIR) && pnpm format

ui-check: ui-lint ui-format ui-dead-code
	$(info UI quality checks passed [lint + format + dead code])

ui-fix: ui-lint-fix ui-format-fix ui-dead-code-fix
	$(info UI quality fixes applied [lint + format + dead code])

ui-dead-code:
	@cd $(UI_DIR) && pnpm dead-code

ui-dead-code-fix:
	@cd $(UI_DIR) && pnpm dead-code:fix

ui-gen-api-types:
	$(info Generating TypeScript types from OpenAPI schema...)
	$(info Exporting OpenAPI schema from FastAPI...)
	@$(VENV_PY) scripts/generate_types.py --openapi
	@cd $(UI_DIR) && pnpm gen:api-types
	@cd $(UI_DIR) && pnpm exec prettier --write src/types/api.generated.ts
	$(info Generated frontend/src/types/api.generated.ts)

ui-gen-ws-types:
	$(info Generating TypeScript types from WebSocket schemas...)
	@$(VENV_PY) scripts/generate_types.py --export
	@cd $(UI_DIR) && pnpm gen:ws-types
	@cd $(UI_DIR) && pnpm exec prettier --write src/types/ws.generated.ts
	$(info Generated frontend/src/types/ws.generated.ts)

ui-gen-zod:
	$(info Generating Zod schemas from WebSocket JSON Schema...)
	@$(VENV_PY) scripts/generate_types.py --frontend-ws
	@cd $(UI_DIR) && pnpm exec prettier --write src/lib/schemas/ws.generated.zod.ts
	$(info Generated frontend/src/lib/schemas/ws.generated.zod.ts)

ui-gen-api-zod:
	$(info Generating Zod schemas from OpenAPI schema...)
	@$(VENV_PY) scripts/generate_types.py --frontend-api
	@cd $(UI_DIR) && pnpm exec prettier --write src/lib/schemas/api.generated.zod.ts
	$(info Generated frontend/src/lib/schemas/api.generated.zod.ts)

ui-gen-entities:
	$(info Generating entity types...)
	@$(VENV_PY) scripts/generate_types.py --entities
	@cd $(UI_DIR) && pnpm exec prettier --write src/types/entities.ts
	$(info Generated frontend/src/types/entities.ts)

ui-gen-types: ui-gen-api-types ui-gen-ws-types ui-gen-zod ui-gen-api-zod ui-gen-entities
	$(info All types generated successfully)

ui-check-types:
	$(info Checking for type drift [non-destructive]...)
	@$(VENV_PY) scripts/check_type_drift.py

ui-test:
	@cd $(UI_DIR) && pnpm test:run

ui-cov:
	@cd $(UI_DIR) && pnpm test:coverage

docs-pdf:
	$(VENV_PY) scripts/build_docs_pdf.py
	$(info Generated snapper.pdf)

ios-setup:
	$(info Setting up iOS project...)
	@command -v xcodegen >/dev/null 2>&1 || { echo "Installing xcodegen..."; brew install xcodegen; }
	cd ios && xcodegen generate
	$(PYTHON) scripts/add_test_target.py ios/Snapper.xcodeproj
	$(MAKE) ios-gen-types
	$(info iOS project setup complete!)

ios-gen-types:
	$(info Generating Swift types from backend schemas...)
	@$(VENV_PY) scripts/generate_types.py --export --ios
	$(info Generated iOS types in ios/Snapper/Models/Generated/)

ios-build:
	$(info Building iOS app...)
	cd ios && xcodebuild -scheme Snapper -destination 'platform=iOS Simulator,name=iPhone 15 Pro,OS=17.5' build

ios-test:
	$(info Running iOS tests...)
	cd ios && xcodebuild -scheme Snapper -destination 'platform=iOS Simulator,name=iPhone 15 Pro,OS=17.5' test

ios-clean:
	$(info Cleaning iOS build artifacts...)
	cd ios && rm -rf DerivedData build

docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-migrate:
	docker run --rm -v "$(CURDIR)/data":/app/data $(IMAGE_NAME):$(IMAGE_TAG) db-init

docker-push:
	docker push $(IMAGE_NAME):$(IMAGE_TAG)

docker-run:
	docker run -d --name snapper --rm -p 8000:8000 -v "$(CURDIR)/data":/app/data $(IMAGE_NAME):$(IMAGE_TAG) server

docker-stop:
	-docker stop snapper
	$(info Container stopped)

server-check:
	@$(VENV_PY) scripts/server_check.py

clean:
	$(PYTHON) scripts/clean.py
