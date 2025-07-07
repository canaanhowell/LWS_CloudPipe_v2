FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Set the default command to run the orchestrator
CMD ["python", "orchestrate_pipeline.py"]