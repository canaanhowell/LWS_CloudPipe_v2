# utils/logger.py

import logging
from datetime import datetime

def log(stage: str, message: str, level: str = "INFO"):
    timestamp = datetime.utcnow().isoformat()
    formatted = f"[{timestamp}] [{level.upper()}] [{stage}] - {message}"
    print(formatted)  # Also log to stdout for visibility
    with open("logs/pipeline.log", "a") as f:
        f.write(formatted + "\n")
