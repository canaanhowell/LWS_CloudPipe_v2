FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV AZURE_CLI_VERSION=2.74.0

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    lsb-release \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Azure CLI
RUN curl -sL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg && \
    echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" > /etc/apt/sources.list.d/azure-cli.list && \
    apt-get update && apt-get install -y azure-cli

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory
RUN mkdir -p /app/logs

# Copy the application code
COPY . .

# Set proper permissions
RUN chmod +x orchestrate_pipeline.py

# Create a startup script for better logging
RUN echo '#!/bin/bash\n\
echo "=== LWS CloudPipe v2 Container Starting ==="\n\
echo "Timestamp: $(date)"\n\
echo "Python version: $(python --version)"\n\
echo "Azure CLI version: $(az --version | head -1)"\n\
echo "Working directory: $(pwd)"\n\
echo "Files in directory: $(ls -la)"\n\
echo "=== Starting Pipeline ==="\n\
python orchestrate_pipeline.py\n\
echo "=== Pipeline Completed ==="\n\
echo "Timestamp: $(date)"\n\
' > /app/start.sh && chmod +x /app/start.sh

# Set the default command
CMD ["/app/start.sh"]