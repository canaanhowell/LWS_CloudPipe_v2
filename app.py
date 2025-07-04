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
    data = request.get_json() or {}
    pipeline = data.get('pipeline', 'default')
    
    return jsonify({
        "status": "started",
        "pipeline": pipeline,
        "job_id": "test_job_123",
        "timestamp": datetime.utcnow().isoformat()
    })

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