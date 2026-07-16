from flask import Flask, jsonify
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({
        "service": "SPAR ETL",
        "status": "running",
        "message": "Hello from Railway!"
    })

@app.route('/health')
def health():
    return jsonify({"status": "healthy"})

@app.route('/products')
def products():
    return jsonify([
        {"id": 1, "name": "Sample Product", "price": 10.99}
    ])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    app.run(host='0.0.0.0', port=port)
