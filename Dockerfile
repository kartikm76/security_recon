FROM python:3.11-slim
WORKDIR /app

# Install system deps if you need them (psycopg2, etc.)
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN pip install --no-cache-dir uv

# Copy project
COPY pyproject.toml uv.lock ./
COPY src ./src

# Install dependencies
RUN uv pip install --system .

# Env
ENV PYTHONPATH=/app/src/main/python/security_recon
ENV SECURITY_RECON_RESOURCES_DIR=/app/src/main/resources

# Expose FastAPI port
EXPOSE 8000

# Default command: run FastAPI
CMD ["uvicorn", "security_recon.controller.api:app", "--host", "0.0.0.0", "--port", "8000"]