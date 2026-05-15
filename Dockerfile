FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

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

# Build Rust extension with ZK features (optional - will fall back to pure Python if build fails)
RUN cd mandala-rust-ext && maturin build --release --features zk || echo "ZK build failed, will use pure Python fallback"

# Install mandala with Rust wheels if available
RUN pip install mandala-rust-ext/target/wheels/mandala_rust_ext-*.whl || echo "Rust extension not available"
RUN pip install .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -fsS http://localhost:8000/healthz || exit 1

CMD ["uvicorn", "mandala.app:app", "--host", "0.0.0.0", "--port", "8000"]
