FROM python:3.12-slim

# Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency definition files
COPY pyproject.toml uv.lock* ./

# Install dependencies system-wide in the container
RUN uv pip install --system .

# Copy the rest of the project files
COPY . .

# Set working directory to src so scripts run seamlessly
WORKDIR /app/src