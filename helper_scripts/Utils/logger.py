# utils/logger.py

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Define valid log levels
VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

class PipelineLogger:
    def __init__(self, log_dir: str = "logs", log_file: str = "pipeline.log"):
        """
        Initialize the pipeline logger.
        
        Args:
            log_dir: Directory to store log files
            log_file: Name of the log file
        """
        self.log_dir = Path(log_dir)
        self.log_file = self.log_dir / log_file
        
        # Ensure log directory exists
        self.log_dir.mkdir(exist_ok=True)
        
        # Set up file handler with rotation
        self._setup_logging()
    
    def _setup_logging(self):
        """Set up the logging configuration."""
        # Create formatter
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Set up file handler with rotation (10MB max, keep 5 backup files)
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            self.log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        
        # Set up console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        # Configure root logger
        self.logger = logging.getLogger('LWS_CloudPipe')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def log(self, stage: str, message: str, level: str = "INFO") -> None:
        """
        Log a message with stage information.
        
        Args:
            stage: The pipeline stage or component name
            message: The log message
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        try:
            # Validate log level
            level_upper = level.upper()
            if level_upper not in VALID_LOG_LEVELS:
                level_upper = "INFO"
                self.logger.warning(f"Invalid log level '{level}', defaulting to INFO")
            
            # Create stage-specific logger
            stage_logger = logging.getLogger(f'LWS_CloudPipe.{stage}')
            stage_logger.setLevel(getattr(logging, level_upper))
            
            # Log the message
            log_method = getattr(stage_logger, level_upper.lower())
            log_method(message)
            
        except Exception as e:
            # Fallback logging if something goes wrong
            timestamp = datetime.utcnow().isoformat()
            fallback_msg = f"[{timestamp}] [ERROR] [LOGGER] - Failed to log message: {str(e)}"
            print(fallback_msg, file=sys.stderr)
    
    def log_json(self, stage: str, data: dict, level: str = "INFO") -> None:
        """
        Log structured data as JSON.
        
        Args:
            stage: The pipeline stage or component name
            data: Dictionary data to log
            level: Log level
        """
        import json
        try:
            timestamp = datetime.utcnow().isoformat()
            log_entry = {
                "timestamp": timestamp,
                "level": level.upper(),
                "stage": stage,
                "data": data
            }
            
            # Write to JSON log file
            json_log_file = self.log_dir / "log.json"
            with open(json_log_file, "a", encoding='utf-8') as f:
                f.write(json.dumps(log_entry) + "\n")
            
            # Also log to regular log
            self.log(stage, f"JSON Data: {json.dumps(data, indent=2)}", level)
            
        except Exception as e:
            self.log("LOGGER", f"Failed to log JSON data: {str(e)}", "ERROR")
    
    def log_progress(self, stage: str, progress: int, total: int, message: str = "") -> None:
        """
        Log progress information.
        
        Args:
            stage: The pipeline stage
            progress: Current progress count
            total: Total count
            message: Additional message
        """
        percentage = (progress / total * 100) if total > 0 else 0
        progress_msg = f"Progress: {progress}/{total} ({percentage:.1f}%)"
        if message:
            progress_msg += f" - {message}"
        
        self.log(stage, progress_msg, "INFO")

# Create a global logger instance
pipeline_logger = PipelineLogger()

# Backward compatibility function
def log(stage: str, message: str, level: str = "INFO") -> None:
    """
    Legacy log function for backward compatibility.
    
    Args:
        stage: The pipeline stage or component name
        message: The log message
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    pipeline_logger.log(stage, message, level)
