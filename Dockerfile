# Use a lightweight Python 3.10 image
FROM python:3.10-slim

# Set working directory
WORKDIR /app
ENV PYTHONPATH=/app

# Install system dependencies (compiler, headers, curl for healthcheck)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    python3-dev \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy dependency list first to leverage Docker layer caching
COPY requirements.txt .

# Upgrade build tools
RUN pip install --upgrade pip setuptools wheel

# Fix compatibility: force NumPy 1.x (since scikit-surprise not yet ready for NumPy 2.x)
RUN pip install "numpy<2"

# Install project dependencies
RUN pip install --no-cache-dir --no-build-isolation -r requirements.txt

# Copy all application files (code, model, csvs, etc.)
COPY . .

# Expose Flask port
EXPOSE 8080

# Healthcheck for container status
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/health || exit 1

# Default command to start the Flask app
CMD ["python", "app.py"]