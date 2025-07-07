#!/usr/bin/env python3
"""
Simple Flask application for testing connection endpoints
"""

from flask import Flask, jsonify, request
import os
from datetime import datetime

app = Flask(__name__)

@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "LWS CloudPipe v2"
    })

@app.route('/api/pipeline/status')
def pipeline_status():
    """Pipeline status endpoint."""
    return jsonify({
        "status": "idle",
        "last_run": None,
        "next_run": None,
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/api/pipeline/run', methods=['POST'])
def run_pipeline():
    """Pipeline run endpoint."""
    import subprocess
    import sys
    from pathlib import Path
    from datetime import datetime

    try:
        # Path to orchestrate_pipeline.py
        script_path = Path(__file__).parent / 'orchestrate_pipeline.py'
        if not script_path.exists():
            return jsonify({
                "status": "error",
                "error": f"Script not found: {script_path}",
                "timestamp": datetime.utcnow().isoformat()
            }), 500

        # Run the orchestrator as a subprocess
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent
        )

        return jsonify({
            "status": "completed" if result.returncode == 0 else "failed",
            "return_code": result.returncode,
            "stdout": result.stdout[-1000:],  # Return last 1000 chars for brevity
            "stderr": result.stderr[-1000:],
            "timestamp": datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }), 500

@app.route('/api/upload/csv', methods=['POST'])
def upload_csv():
    """CSV upload endpoint."""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400
    
    # For testing, just return success
    return jsonify({
        "status": "uploaded",
        "filename": file.filename,
        "size": len(file.read()),
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/api/download/<filename>')
def download_file(filename):
    """Download endpoint."""
    # For testing, return a simple response
    return jsonify({
        "status": "available",
        "filename": filename,
        "url": f"/api/download/{filename}",
        "timestamp": datetime.utcnow().isoformat()
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True) 