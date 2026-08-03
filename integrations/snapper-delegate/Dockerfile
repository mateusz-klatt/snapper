# Minimal blackbox delegate image.
#
# Ships ONLY the agent-plane package and its runtime dependencies — no Snapper
# source, no migrations, no seeds, no credentials. A compromised model in this
# container has no backend code to whitebox-hack and nothing to read. The build
# context is this directory (integrations/snapper-delegate) alone, so no backend
# source can leak in; .dockerignore drops the tests and the backend-coupled
# registration.py (unused in the runner-only PID1 deployment).
#
# Build (from this directory):
#   docker build -t snapper-delegate:blackbox integrations/snapper-delegate
# Run posture (compose supplies read_only, cap_drop, network, secrets):
#   entrypoint python -m snapper_delegate.pid1

FROM python:3.14-slim AS build
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY requirements.lock /tmp/requirements.lock
RUN python -m pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/requirements.lock

FROM python:3.14-slim AS runtime
RUN useradd --uid 888 --create-home --shell /usr/sbin/nologin delegate
COPY --from=build /opt/venv /opt/venv
COPY src/snapper_delegate /app/src/snapper_delegate
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONNOUSERSITE=1 \
    PYTHONSAFEPATH=1 \
    PYTHONUNBUFFERED=1
USER 888:888
ENTRYPOINT ["python", "-m", "snapper_delegate.pid1"]
