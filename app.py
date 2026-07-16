"""
SPAR ETL RECEIVER - SUPABASE (POSTGRESQL) VERSION
For Railway Deployment
"""

import os
import sys
import logging
import socket
from datetime import datetime, date, time
from decimal import Decimal
import random
import traceback

# Force IPv4
try:
    original_getaddrinfo = socket.getaddrinfo
    
    def ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
    
    socket.getaddrinfo = ipv4_only_getaddrinfo
except Exception as e:
    pass

from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

logger.info("🚀 SPAR ETL Receiver starting...")

app = Flask(__name__)

# ============================================
# DATABASE CONNECTION - SUPABASE
# ============================================

SUPABASE_HOST = os.environ.get('SUPABASE_HOST', 'db.livwipmybrvgtgrbtxkc.supabase.co')
SUPABASE_DATABASE = os.environ.get('SUPABASE_DATABASE', 'postgres')
SUPABASE_USERNAME = os.environ.get('SUPABASE_USERNAME', 'postgres')
SUPABASE_PASSWORD = os.environ.get('SUPABASE_PASSWORD', 'W2QjDGkLDNOy87OC')
SUPABASE_PORT = os.environ.get('SUPABASE_PORT', '5432')

logger.info(f"📡 Supabase Host: {SUPABASE_HOST}")
logger.info(f"📡 Database: {SUPABASE_DATABASE}")
logger.info(f"📡 Username: {SUPABASE_USERNAME}")
logger.info(f"📡 Port: {SUPABASE_PORT}")

def get_db_connection():
    """Get connection to Supabase PostgreSQL"""
    try:
        # Resolve host to IPv4
        try:
            host_ip = socket.gethostbyname(SUPABASE_HOST)
            logger.info(f"✅ Resolved {SUPABASE_HOST} to IPv4: {host_ip}")
            connect_host = host_ip
        except Exception as e:
            logger.warning(f"Could not resolve host to IPv4: {e}")
            connect_host = SUPABASE_HOST
        
        conn_str = f"postgresql://{SUPABASE_USERNAME}:{SUPABASE_PASSWORD}@{connect_host}:{SUPABASE_PORT}/{SUPABASE_DATABASE}"
        logger.info(f"Connecting to {connect_host}:{SUPABASE_PORT}...")
        
        conn = psycopg2.connect(conn_str, connect_timeout=30)
        logger.info("✅ Supabase connection successful!")
        return conn
    except Exception as e:
        logger.error(f"❌ Supabase connection failed: {e}")
        return None

# ============================================
# ROUTES
# ============================================

@app.route('/', methods=['GET'])
def index():
    conn = get_db_connection()
    db_status = "connected" if conn else "disconnected"
    if conn:
        conn.close()
    
    return jsonify({
        "service": "SPAR ETL Receiver - Supabase",
        "status": "running",
        "database": SUPABASE_DATABASE,
        "db_status": db_status,
        "endpoints": {
            "health": "GET /health",
            "products": "GET /products",
            "products/add": "POST /products/add",
            "sales_orders": "GET /sales-orders, POST /sales-orders",
            "purchase_orders": "GET /purchase-orders, POST /purchase-orders",
            "recent": "GET /recent",
            "receipt/<order_number>": "GET /receipt/<order_number>"
        }
    })

@app.route('/health', methods=['GET'])
def health():
    """Health check for Railway"""
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            conn.close()
            return jsonify({"status": "healthy", "database": "connected"}), 200
        return jsonify({"status": "degraded", "database": "disconnected"}), 200
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/products', methods=['GET'])
def get_products():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                p.id, p.product_code, p.product_name, pc.category_name,
                p.unit_price, p.current_stock, p.reorder_level
            FROM erp_products p
            LEFT JOIN erp_product_categories pc ON p.category_id = pc.id
            WHERE p.is_active = 1
            ORDER BY pc.category_name, p.product_name
            LIMIT 100
        """)
        
        rows = []
        for row in cursor.fetchall():
            rows.append({
                "id": row[0],
                "product_code": row[1],
                "product_name": row[2],
                "category_name": row[3],
                "unit_price": float(row[4]) if row[4] else 0,
                "current_stock": row[5],
                "reorder_level": row[6]
            })
        cursor.close()
        conn.close()
        return jsonify(rows), 200
        
    except Exception as e:
        logger.error(f"Products error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/recent', methods=['GET'])
def get_recent_sales():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                so.so_number as sale_id,
                c.customer_name,
                so.order_date as sale_date,
                TO_CHAR(so.order_time, 'HH24:MI:SS') as sale_time,
                so.total_amount as total_sales,
                COALESCE(so.rewards_earned, 0) as rewards_earned,
                so.created_by as recorded_by
            FROM erp_sales_orders so
            LEFT JOIN erp_customers c ON so.customer_id = c.id
            ORDER BY so.created_at DESC
            LIMIT 50
        """)

        rows = []
        for row in cursor.fetchall():
            rows.append({
                "sale_id": row[0],
                "customer_name": row[1],
                "sale_date": row[2].strftime('%Y-%m-%d') if row[2] else '',
                "sale_time": row[3] or '',
                "total_sales": float(row[4]) if row[4] else 0,
                "rewards_earned": float(row[5]) if row[5] else 0,
                "recorded_by": row[6] or ''
            })
        cursor.close()
        conn.close()
        return jsonify(rows), 200

    except Exception as e:
        logger.error(f"Recent sales error: {e}")
        return jsonify([]), 200

@app.route('/sales-orders', methods=['GET'])
def get_sales_orders():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                so.so_number as order_number,
                c.customer_name,
                so.order_date,
                TO_CHAR(so.order_time, 'HH24:MI:SS') as order_time,
                so.total_amount,
                so.status,
                so.approval_status,
                so.created_by as recorded_by,
                COALESCE(so.rewards_earned, 0) as rewards_earned
            FROM erp_sales_orders so
            LEFT JOIN erp_customers c ON so.customer_id = c.id
            ORDER BY so.created_at DESC
            LIMIT 100
        """)
        
        rows = []
        for row in cursor.fetchall():
            rows.append({
                "order_number": row[0],
                "customer_name": row[1],
                "order_date": row[2].strftime('%Y-%m-%d') if row[2] else '',
                "order_time": row[3] or '',
                "total_amount": float(row[4]) if row[4] else 0,
                "status": row[5] or 'Draft',
                "approval_status": row[6] or 'Pending',
                "recorded_by": row[7] or '',
                "rewards_earned": float(row[8]) if row[8] else 0
            })
        cursor.close()
        conn.close()
        return jsonify(rows), 200
        
    except Exception as e:
        logger.error(f"Sales orders error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/sales-orders', methods=['POST'])
def create_sales_order():
    try:
        data = request.json
        logger.info("📝 Creating sales order...")
        
        order_number = 'SO-' + datetime.now().strftime('%Y%m%d') + '-' + str(random.randint(1000, 9999))
        items = data.get('items', [])

        if not items:
            return jsonify({"error": "No items in sales order"}), 400

        subtotal = sum(safe_float(item.get('quantity', 0)) * safe_float(item.get('unit_price', 0)) for item in items)
        tax = subtotal * 0.155
        total = subtotal + tax
        rewards_earned = total * 0.02

        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        try:
            customer_name = data['customer_name'].strip()
            customer_email = data.get('customer_email', '').strip()

            cursor.execute("SELECT id, rewards_balance FROM erp_customers WHERE customer_name = %s", [customer_name])
            customer = cursor.fetchone()

            if customer:
                customer_id = customer[0]
                current_rewards = safe_float(customer[1] or 0)
            else:
                customer_code = 'CUST-' + datetime.now().strftime('%Y%m%d%H%M%S')
                cursor.execute("""
                    INSERT INTO erp_customers (customer_code, customer_name, customer_type, email, is_active, rewards_balance, created_by, created_at)
                    VALUES (%s, %s, %s, %s, 1, 0, %s, NOW())
                    RETURNING id
                """, [customer_code, customer_name, 'Retail', customer_email, data.get('recorded_by', 'system')])
                customer_id = cursor.fetchone()[0]
                current_rewards = 0

            cursor.execute("""
                INSERT INTO erp_sales_orders (
                    so_number, customer_id, order_date, order_time,
                    subtotal, tax_amount, total_amount, rewards_earned,
                    status, approval_status, created_by, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """, (
                order_number, customer_id,
                datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%H:%M:%S'),
                safe_float(subtotal), safe_float(tax), safe_float(total), safe_float(rewards_earned),
                'Confirmed', 'Approved', data.get('recorded_by', 'system')
            ))
            order_id = cursor.fetchone()[0]
            logger.info(f"✅ Order inserted, ID: {order_id}")

            for i, item in enumerate(items):
                product_id = item.get('product_id')
                if not product_id:
                    continue

                cursor.execute("SELECT product_code, product_name FROM erp_products WHERE id = %s", [product_id])
                product = cursor.fetchone()
                if not product:
                    continue

                product_code, product_name = product[0], product[1]
                qty = safe_float(item.get('quantity', 0))
                price = safe_float(item.get('unit_price', 0))
                line_total = qty * price

                cursor.execute("""
                    INSERT INTO erp_sales_order_lines (
                        so_id, line_number, product_id, product_code, product_name,
                        quantity, unit_price, line_total
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_id, i + 1, product_id, product_code, product_name, qty, price, line_total))

                cursor.execute("UPDATE erp_products SET current_stock = current_stock - %s WHERE id = %s", (qty, product_id))

            new_rewards = safe_float(current_rewards) + safe_float(rewards_earned)
            cursor.execute("UPDATE erp_customers SET rewards_balance = %s WHERE id = %s", (new_rewards, customer_id))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ Sales order {order_number} created")
            return jsonify({
                "status": "success",
                "order_number": order_number,
                "total_amount": safe_float(total),
                "rewards_earned": safe_float(rewards_earned),
                "customer_rewards_total": safe_float(new_rewards),
                "items_inserted": len(items)
            }), 200

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Sales order failed: {e}")
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        logger.error(f"❌ Sales order error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# HELPER FUNCTIONS
# ============================================

def safe_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
    return response

# ============================================
# MAIN - CRITICAL: Correct binding for Railway
# ============================================

if __name__ == '__main__':
    # Railway injects PORT environment variable
    port = int(os.environ.get('PORT', 8000))
    
    print("=" * 70)
    print("🛒 SPAR ETL RECEIVER - SUPABASE VERSION")
    print("=" * 70)
    print(f"\n🚀 Starting on port {port}...")
    print(f"📡 Bind address: 0.0.0.0 (all interfaces)")
    print(f"📡 Supabase Host: {SUPABASE_HOST}")
    print(f"📡 Database: {SUPABASE_DATABASE}")
    print("=" * 70)
    print(f"\n✅ Server will be accessible on PORT {port}")
    print("=" * 70)
    
    # CRITICAL: Bind to 0.0.0.0 and PORT
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
