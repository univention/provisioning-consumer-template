# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency manifests first for better layer caching
COPY lib/pyproject.toml lib/
COPY lib/src/ lib/src/

# Install the library (with its dependencies) into an isolated venv
RUN uv venv .venv && \
    uv pip install --python .venv/bin/python ./lib

# Copy application source and install the package itself (no deps, already installed)
COPY pyproject.toml .
COPY src/ src/
RUN uv pip install --python .venv/bin/python --no-deps .

# Fix symlink: distroless python3 image has the Python executable in /usr/bin,
# while in python:3.12-slim it's in /usr/local/bin.
RUN ln -sf /usr/bin/python3 /app/.venv/bin/python && \
    ln -sf /usr/bin/python3 /app/.venv/bin/python3

# ── Runtime stage ─────────────────────────────────────────────────────────────
# gcr.io/distroless/python3 bundles CPython but no shell, package manager, etc.
FROM gcr.io/distroless/python3-debian12:nonroot

WORKDIR /app

# Copy the populated venv from the builder
COPY --from=builder /app/.venv /app/.venv

# Make the venv's Python the default interpreter path used by the entry point
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/.venv/lib/python3.12/site-packages"

# distroless has no shell, so use the exec form and invoke Python directly
# alternative: ENTRYPOINT ["/app/.venv/bin/python3", "-m", "provisioning_consumer.main"]
ENTRYPOINT ["/app/.venv/bin/provisioning-consumer"]
