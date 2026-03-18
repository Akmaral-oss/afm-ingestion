FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install necessary tools and Rust
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libssl-dev \
    libffi-dev \
    cargo \
    uv

# Install Rust and Cargo (this ensures it's installed even if the package manager is outdated)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Create the /booking directory and set as working directory
RUN mkdir /afm-ingestion
WORKDIR /afm-ingestion

# Copy the requirements and install dependencies
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev --no-install-project

# Copy the rest of the application files
COPY . .

# Ensure shell scripts in the docker folder are executable
RUN chmod a+x /booking/docker/*.sh

# Start the application with Gunicorn using Uvicorn worker
CMD []
