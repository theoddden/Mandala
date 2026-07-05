# Build stage
FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Install build dependencies including Rust for optional ZK features
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY mandala-rust-ext/ ./mandala-rust-ext/

# Install maturin for building Rust extension
RUN pip install --upgrade pip maturin

# Build Rust extension with ZK features (optional - falls back to pure Python if build fails)
RUN cd mandala-rust-ext && maturin build --release --features zk 2>&1 | tail -5 || true

# Install Rust wheel only if one was actually produced; true no-op otherwise
RUN find mandala-rust-ext/target/wheels/ -name "mandala_rust_ext-*.whl" | head -1 | xargs pip install 2>/dev/null || echo "Rust extension not available, using pure Python fallback"

# Install Python package
RUN pip install . --target /install

# Runtime stage
FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Copy installed dependencies from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY src/ ./src/
COPY pyproject.toml README.md ./

# Create non-root user
RUN groupadd -r mandala && useradd -r -g mandala mandala \
    && chown -R mandala:mandala /app

USER mandala

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

CMD ["uvicorn", "mandala.app:app", "--host", "0.0.0.0", "--port", "8000"]
