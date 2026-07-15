"""
SPAR ETL RECEIVER - SUPABASE (POSTGRESQL) VERSION
- Works with Supabase PostgreSQL
- Uses psycopg2 driver
- Two-step ID retrieval: insert, then query by unique number
"""

from flask import Flask, request, jsonify
from datetime import datetime, date, time
import psycopg2
import psycopg2.extras
import logging
import os
import random
import traceback
from decimal import Decimal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# ============================================
# DATABASE CONNECTION - SUPABASE
# ============================================

# Supabase Connection Settings (set these as environment variables)
SUPABASE_HOST = os.environ.get('SUPABASE_HOST', 'db.livwipmybrvgtgrbtxkc.supabase.co')
SUPABASE_DATABASE = os.environ.get('SUPABASE_DATABASE', 'postgres')
SUPABASE_USERNAME = os.environ.get('SUPABASE_USERNAME', 'postgres')
SUPABASE_PASSWORD = os.environ.get('SUPABASE_PASSWORD', 'W2QjDGkLDNOy87OC')
SUPABASE_PORT = os.environ.get('SUPABASE_PORT', '5432')

# Connection string
DATABASE_URL = f"postgresql://{SUPABASE_USERNAME}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DATABASE}"

def get_db_connection():
    """Get connection to Supabase PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("✅ Supabase connection successful!")
        return conn
    except Exception as e:
        logger.error(f"❌ Supabase connection failed: {e}")
        logger.error(f"Host: {SUPABASE_HOST}")
        logger.error(f"Database: {SUPABASE_DATABASE}")
        logger.error(f"Username: {SUPABASE_USERNAME}")
        return None

# Test connection on startup
def test_db_on_startup():
    """Test database connection on startup"""
    logger.info("Testing Supabase connection...")
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT current_database(), version()")
            row = cursor.fetchone()
            logger.info(f"✅ Connected to database: {row[0]}")
            logger.info(f"✅ PostgreSQL Version: {row[1][:100]}...")
            
            # Check if tables exist
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('erp_products', 'erp_customers', 'erp_sales_orders')
            """)
            table_count = cursor.fetchone()[0]
            logger.info(f"📊 Found {table_count} main tables")
            
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"❌ Startup test failed: {e}")
            conn.close()
            return False
    return False

# Run startup test
test_db_on_startup()

# ============================================
# HELPER FUNCTIONS
# ============================================

def safe_float(value):
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def convert_row_to_serializable(row, columns):
    row_dict = {}
    for i, col in enumerate(columns):
        value = row[i]
        if isinstance(value, time):
            value = value.strftime('%H:%M:%S')
        elif isinstance(value, datetime):
            value = value.isoformat()
        elif isinstance(value, date):
            value = value.isoformat()
        elif isinstance(value, Decimal):
            value = float(value)
        row_dict[col] = value
    return row_dict

def get_default_bank_account_id(cursor):
    cursor.execute("SELECT id FROM erp_bank_accounts WHERE is_active = 1 LIMIT 1")
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("""
        INSERT INTO erp_bank_accounts (account_name, account_number, bank_name, balance, currency, is_active, created_by, created_at)
        VALUES ('Cash Account', 'CASH001', 'Default Bank', 0, 'USD', 1, 'system', NOW())
        RETURNING id
    """)
    return cursor.fetchone()[0]

def update_cash_balance(cursor, amount, description, reference, transaction_type):
    account_id = get_default_bank_account_id(cursor)
    cursor.execute("""
        UPDATE erp_bank_accounts
        SET balance = COALESCE(balance, 0) + %s
        WHERE id = %s
    """, (safe_float(amount), account_id))
    try:
        cursor.execute("""
            INSERT INTO erp_cash_transactions (
                bank_account_id, transaction_date, description, amount,
                transaction_type, reference, created_by, created_at
            ) VALUES (%s, NOW(), %s, %s, %s, %s, %s, NOW())
        """, (account_id, description, abs(safe_float(amount)),
               'credit' if amount > 0 else 'debit',
               reference, 'system'))
    except Exception as e:
        logger.warning(f"Could not insert cash transaction: {e}")

# ============================================
# ROOT
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
            "test": "GET /test",
            "debug": "GET /debug",
            "products": "GET /products",
            "products/add": "POST /products/add",
            "sales_orders": "GET /sales-orders, POST /sales-orders",
            "purchase_orders": "GET /purchase-orders, POST /purchase-orders",
            "purchase_orders/lines": "GET /purchase-orders/<po_number>/lines",
            "purchase_orders/<po_number>/approve": "POST",
            "purchase_orders/<po_number>/reject": "POST",
            "purchase_orders/<po_number>": "DELETE",
            "goods_receipt": "POST /goods-receipt",
            "recent": "GET /recent",
            "bank_accounts": "GET /bank-accounts",
            "bank_balance": "GET /bank-balance",
            "dynamic_cash_balance": "GET /dynamic-cash-balance",
            "customers": "GET /customers",
            "receipt/<order_number>": "GET /receipt/<order_number>",
            "overdue_pos": "GET /overdue-pos",
            "incoming_documents": "GET /incoming-documents",
            "pending_approvals": "GET /pending-approvals",
            "unprocessed_payments": "GET /unprocessed-payments"
        }
    })

# ============================================
# DEBUG
# ============================================

@app.route('/debug', methods=['GET'])
def debug():
    try:
        conn = get_db_connection()
        db_status = "connected" if conn else "disconnected"
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM erp_products")
            product_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        else:
            product_count = 0
            
        return jsonify({
            "status": "ok",
            "database": db_status,
            "product_count": product_count,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================
# HEALTH
# ============================================

@app.route('/health', methods=['GET'])
def health():
    try:
        conn = get_db_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM erp_products")
            product_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            db_status = "connected"
        else:
            db_status = "disconnected"
            product_count = 0
    except Exception as e:
        db_status = f"error: {str(e)[:50]}"
        product_count = 0

    return jsonify({
        "status": "healthy" if db_status == "connected" else "degraded",
        "database": db_status,
        "product_count": product_count,
        "timestamp": datetime.now().isoformat(),
        "service": "SPAR ETL Receiver - Supabase"
    })

# ============================================
# TEST
# ============================================

@app.route('/test', methods=['GET'])
def test():
    return jsonify({"status": "ok", "message": "Server is running!"})

# ============================================
# PRODUCTS
# ============================================

@app.route('/products', methods=['GET'])
def get_products():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        query = """
            SELECT 
                p.id, p.product_code, p.product_name, pc.category_name,
                p.unit_of_measure, p.unit_price, p.cost_price,
                p.current_stock, p.reorder_level,
                p.current_stock AS available_stock,
                CASE 
                    WHEN p.current_stock <= 0 THEN 'out-of-stock'
                    WHEN p.current_stock <= p.reorder_level THEN 'low-stock'
                    ELSE 'in-stock'
                END AS stock_status,
                p.is_active
            FROM erp_products p
            LEFT JOIN erp_product_categories pc ON p.category_id = pc.id
            WHERE p.is_active = 1
            ORDER BY pc.category_name, p.product_name
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()
        return jsonify(rows), 200
        
    except Exception as e:
        logger.error(f"Products error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/products/add', methods=['POST'])
def add_product():
    try:
        data = request.json
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        cursor = conn.cursor()
        category_name = data.get('category_name', '').strip()
        if not category_name:
            return jsonify({"error": "Category is required"}), 400
            
        cursor.execute("SELECT id FROM erp_product_categories WHERE category_name = %s", [category_name])
        cat = cursor.fetchone()
        if cat:
            category_id = cat[0]
        else:
            cursor.execute("INSERT INTO erp_product_categories (category_name, created_by, created_at) VALUES (%s, %s, NOW()) RETURNING id", 
                          [category_name, data.get('created_by', 'system')])
            conn.commit()
            category_id = cursor.fetchone()[0]
            
        insert_query = """
            INSERT INTO erp_products (
                product_code, product_name, category_id, unit_of_measure,
                unit_price, cost_price, current_stock, reorder_level,
                is_active, created_by, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id
        """
        params = (
            data.get('product_code'),
            data.get('product_name'),
            category_id,
            data.get('unit_of_measure', 'EA'),
            safe_float(data.get('unit_price', 0)),
            safe_float(data.get('cost_price', 0)),
            int(data.get('initial_stock', 0)),
            int(data.get('reorder_level', 10)),
            1,
            data.get('created_by', 'system')
        )
        cursor.execute(insert_query, params)
        product_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "success", "id": product_id}), 200
        
    except Exception as e:
        logger.error(f"Add product error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# SALES ORDERS (GET)
# ============================================

@app.route('/sales-orders', methods=['GET'])
def get_sales_orders():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        query = """
            SELECT 
                so.so_number as order_number,
                c.customer_name,
                so.order_date,
                TO_CHAR(so.order_time, 'HH24:MI:SS') as order_time,
                so.total_amount,
                so.status,
                so.approval_status,
                so.created_by as recorded_by,
                COALESCE(so.rewards_earned, 0) as rewards_earned,
                COALESCE(c.rewards_balance, 0) as rewards_balance
            FROM erp_sales_orders so
            LEFT JOIN erp_customers c ON so.customer_id = c.id
            ORDER BY so.created_at DESC
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()
        return jsonify(rows), 200
        
    except Exception as e:
        logger.error(f"Sales orders error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# SALES ORDERS (POST)
# ============================================

@app.route('/sales-orders', methods=['POST'])
def create_sales_order():
    try:
        data = request.json
        logger.info("=" * 60)
        logger.info("📝 Creating sales order")
        logger.info(f"👤 Customer: {data.get('customer_name')}")
        logger.info(f"📦 Items received: {data.get('items')}")
        logger.info("=" * 60)

        order_number = 'SO-' + datetime.now().strftime('%Y%m%d') + '-' + str(random.randint(1000, 9999))
        items = data.get('items', [])

        if not items:
            return jsonify({"error": "No items in sales order"}), 400

        subtotal = 0.0
        for item in items:
            qty = safe_float(item.get('quantity', 0))
            price = safe_float(item.get('unit_price', 0))
            subtotal += qty * price
        
        tax = subtotal * 0.155
        total = subtotal + tax
        rewards_earned = total * 0.02

        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        try:
            # Customer logic
            customer_name = data['customer_name'].strip()
            customer_email = data.get('customer_email', '').strip()

            customer_query = "SELECT id, rewards_balance FROM erp_customers WHERE customer_name = %s"
            cursor.execute(customer_query, [customer_name])
            customer = cursor.fetchone()

            if customer:
                customer_id = customer[0]
                current_rewards = safe_float(customer[1] or 0)
            else:
                customer_code = 'CUST-' + datetime.now().strftime('%Y%m%d%H%M%S')
                insert_customer = """
                    INSERT INTO erp_customers (customer_code, customer_name, customer_type, email, is_active, rewards_balance, created_by, created_at)
                    VALUES (%s, %s, %s, %s, 1, 0, %s, NOW())
                    RETURNING id
                """
                cursor.execute(insert_customer, [customer_code, customer_name, 'Retail', customer_email, data.get('recorded_by', 'system')])
                customer_id = cursor.fetchone()[0]
                current_rewards = 0

            # Insert order header
            insert_order_query = """
                INSERT INTO erp_sales_orders (
                    so_number, customer_id, order_date, order_time,
                    subtotal, tax_amount, total_amount, rewards_earned,
                    status, approval_status, created_by, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """
            order_params = (
                order_number, customer_id,
                datetime.now().strftime('%Y-%m-%d'), datetime.now().strftime('%H:%M:%S'),
                safe_float(subtotal), safe_float(tax), safe_float(total), safe_float(rewards_earned),
                'Confirmed', 'Approved', data.get('recorded_by', 'system')
            )
            cursor.execute(insert_order_query, order_params)
            order_id = cursor.fetchone()[0]
            logger.info(f"✅ Order header inserted, ID: {order_id}")

            # Insert lines
            for i, item in enumerate(items):
                product_id = item.get('product_id')
                if not product_id:
                    raise Exception(f"Item {i+1} has no product_id")

                cursor.execute("SELECT id, product_code, product_name FROM erp_products WHERE id = %s", [product_id])
                product = cursor.fetchone()
                if not product:
                    raise Exception(f"Product ID {product_id} not found")

                product_code, product_name = product[1], product[2]
                qty = safe_float(item.get('quantity', 0))
                price = safe_float(item.get('unit_price', 0))
                line_total = qty * price

                line_query = """
                    INSERT INTO erp_sales_order_lines (
                        so_id, line_number, product_id, product_code, product_name,
                        quantity, unit_price, line_total
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(line_query, (
                    order_id, i + 1, product_id, product_code, product_name,
                    qty, price, line_total
                ))
                logger.info(f"✅ Inserted line {i+1}: {product_name} x {qty} = ${line_total:.2f}")

                cursor.execute("UPDATE erp_products SET current_stock = current_stock - %s WHERE id = %s", (qty, product_id))

            # Update rewards
            new_rewards = safe_float(current_rewards) + safe_float(rewards_earned)
            cursor.execute("UPDATE erp_customers SET rewards_balance = %s WHERE id = %s", (new_rewards, customer_id))

            # Update cash balance
            update_cash_balance(cursor, total, f"Cash sale {order_number}", order_number, 'credit')

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ Sales order {order_number} created with {len(items)} lines")
            logger.info("=" * 60)

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
            logger.error(f"❌ Sales order transaction failed: {e}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        logger.error(f"❌ Sales order error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# PURCHASE ORDERS (GET)
# ============================================

@app.route('/purchase-orders', methods=['GET'])
def get_purchase_orders():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        query = """
            SELECT 
                po.po_number,
                s.supplier_name,
                po.order_date,
                po.expected_delivery_date,
                po.total_amount,
                po.status,
                po.approval_status,
                po.created_by
            FROM erp_purchase_orders po
            LEFT JOIN erp_suppliers s ON po.supplier_id = s.id
            ORDER BY po.created_at DESC
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()
        return jsonify(rows), 200
        
    except Exception as e:
        logger.error(f"Purchase orders error: {e}")
        return jsonify([]), 200

# ============================================
# PURCHASE ORDERS (POST)
# ============================================

@app.route('/purchase-orders', methods=['POST'])
def create_purchase_order():
    try:
        data = request.json
        logger.info("=" * 60)
        logger.info("📦 Creating purchase order")
        logger.info(f"🏢 Supplier: {data.get('supplier_name')}")
        logger.info(f"📦 Items received: {data.get('items')}")
        logger.info("=" * 60)

        po_number = 'PO-' + datetime.now().strftime('%Y%m%d') + '-' + str(random.randint(1000, 9999))
        items = data.get('items', [])

        if not items:
            return jsonify({"error": "No items in purchase order"}), 400

        subtotal = 0.0
        for item in items:
            qty = safe_float(item.get('quantity', 0))
            price = safe_float(item.get('unit_price', 0))
            subtotal += qty * price
        
        tax = subtotal * 0.155
        total = subtotal + tax

        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        try:
            # Supplier logic
            supplier_name = data['supplier_name'].strip()
            supplier_email = data.get('supplier_email', '').strip()

            supplier_query = "SELECT id FROM erp_suppliers WHERE supplier_name = %s"
            cursor.execute(supplier_query, [supplier_name])
            supplier = cursor.fetchone()

            if supplier:
                supplier_id = supplier[0]
            else:
                supplier_code = 'SUP-' + datetime.now().strftime('%Y%m%d%H%M%S')
                insert_supplier = """
                    INSERT INTO erp_suppliers (supplier_code, supplier_name, email, is_active, created_by, created_at)
                    VALUES (%s, %s, %s, 1, %s, NOW())
                    RETURNING id
                """
                cursor.execute(insert_supplier, [supplier_code, supplier_name, supplier_email, data.get('created_by', 'system')])
                supplier_id = cursor.fetchone()[0]

            # Insert PO header
            insert_po_query = """
                INSERT INTO erp_purchase_orders (
                    po_number, supplier_id, order_date,
                    expected_delivery_date, subtotal, tax_amount, total_amount,
                    status, approval_status, created_by, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """
            po_params = (
                po_number, supplier_id,
                datetime.now().strftime('%Y-%m-%d'),
                data.get('expected_delivery_date'),
                safe_float(subtotal), safe_float(tax), safe_float(total),
                'Draft', 'Pending', data.get('created_by', 'system')
            )
            cursor.execute(insert_po_query, po_params)
            po_id = cursor.fetchone()[0]
            logger.info(f"✅ PO header inserted, ID: {po_id}")

            # Insert lines
            for i, item in enumerate(items):
                product_id = item.get('product_id')
                if not product_id:
                    raise Exception(f"Item {i+1} has no product_id")

                cursor.execute("SELECT id, product_code, product_name FROM erp_products WHERE id = %s", [product_id])
                product = cursor.fetchone()
                if not product:
                    raise Exception(f"Product ID {product_id} not found")

                product_code, product_name = product[1], product[2]
                qty = safe_float(item.get('quantity', 0))
                price = safe_float(item.get('unit_price', 0))
                line_total = qty * price

                line_query = """
                    INSERT INTO erp_purchase_order_lines (
                        po_id, line_number, product_id, product_code, product_name,
                        quantity, unit_price, line_total
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(line_query, (
                    po_id, i + 1, product_id, product_code, product_name,
                    qty, price, line_total
                ))
                logger.info(f"✅ Inserted PO line {i+1}: {product_name} x {qty} = ${line_total:.2f}")

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ PO {po_number} created with {len(items)} lines")
            logger.info("=" * 60)

            return jsonify({
                "status": "success",
                "po_number": po_number,
                "total_amount": safe_float(total),
                "po_id": po_id,
                "lines_inserted": len(items)
            }), 200

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ PO transaction failed: {e}")
            traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        logger.error(f"❌ PO error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# PURCHASE ORDER LINES
# ============================================

@app.route('/purchase-orders/<po_number>/lines', methods=['GET'])
def get_purchase_order_lines(po_number):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500
            
        query = """
            SELECT pol.id, pol.product_id, pol.product_code, pol.product_name,
                   pol.quantity, pol.unit_price, pol.line_total
            FROM erp_purchase_order_lines pol
            INNER JOIN erp_purchase_orders po ON pol.po_id = po.id
            WHERE po.po_number = %s
            ORDER BY pol.line_number
        """
        
        cursor = conn.cursor()
        cursor.execute(query, [po_number])
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()
        return jsonify(rows), 200
        
    except Exception as e:
        logger.error(f"PO lines error: {e}")
        return jsonify([]), 200

# ============================================
# GOODS RECEIPT
# ============================================

@app.route('/goods-receipt', methods=['POST'])
def receive_goods():
    try:
        data = request.json
        po_number = data.get('po_number')
        logger.info(f"📥 Receiving goods for PO: {po_number}")

        items = data.get('items', [])
        
        if not items:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM erp_purchase_orders WHERE po_number = %s", [po_number])
                po = cursor.fetchone()
                if po:
                    po_id = po[0]
                    cursor.execute("""
                        SELECT product_id, product_code, product_name, quantity, unit_price as unit_cost
                        FROM erp_purchase_order_lines WHERE po_id = %s
                    """, [po_id])
                    lines = cursor.fetchall()
                    for line in lines:
                        items.append({
                            "product_id": line[0],
                            "product_code": line[1] or '',
                            "product_name": line[2] or '',
                            "quantity": safe_float(line[3] or 0),
                            "unit_cost": safe_float(line[4] or 0)
                        })
                cursor.close()
                conn.close()
        
        if not items:
            return jsonify({
                "error": "No items to receive for this PO",
                "po_number": po_number
            }), 400

        receipt_number = 'GRN-' + datetime.now().strftime('%Y%m%d') + '-' + str(random.randint(1000, 9999))

        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        try:
            po_query = "SELECT id, supplier_id FROM erp_purchase_orders WHERE po_number = %s"
            cursor.execute(po_query, [po_number])
            po = cursor.fetchone()
            if not po:
                return jsonify({"error": "Purchase order not found"}), 404

            po_id = po[0]
            supplier_id = po[1]

            total_quantity = sum(safe_float(item.get('quantity', 0)) for item in items)
            total_cost = sum(safe_float(item.get('quantity', 0)) * safe_float(item.get('unit_cost', 0)) for item in items)

            receipt_query = """
                INSERT INTO erp_goods_receipts (
                    receipt_number, po_id, supplier_id, receipt_date,
                    total_quantity, total_cost, status, created_by, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """
            cursor.execute(receipt_query, (
                receipt_number, po_id, supplier_id,
                datetime.now().strftime('%Y-%m-%d'),
                safe_float(total_quantity), safe_float(total_cost), 'Completed', data.get('created_by', 'system')
            ))
            receipt_id = cursor.fetchone()[0]

            for i, item in enumerate(items):
                product_id = item.get('product_id')
                if not product_id:
                    product_code = item.get('product_code', '')
                    product_name = item.get('product_name', '')
                    if product_code:
                        cursor.execute("SELECT id FROM erp_products WHERE product_code = %s", [product_code])
                        prod = cursor.fetchone()
                        if prod:
                            product_id = prod[0]
                    if not product_id and product_name:
                        cursor.execute("SELECT id FROM erp_products WHERE product_name = %s", [product_name])
                        prod = cursor.fetchone()
                        if prod:
                            product_id = prod[0]
                    if not product_id:
                        logger.error(f"❌ Cannot find product for item: {item}")
                        continue

                line_query = """
                    INSERT INTO erp_goods_receipt_lines (
                        receipt_id, line_number, product_id, product_code, product_name,
                        quantity, unit_cost, total_cost
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(line_query, (
                    receipt_id, i + 1, product_id,
                    str(item.get('product_code', '')),
                    str(item.get('product_name', '')),
                    safe_float(item.get('quantity', 0)),
                    safe_float(item.get('unit_cost', 0)),
                    safe_float(item.get('quantity', 0)) * safe_float(item.get('unit_cost', 0))
                ))

                cursor.execute("""
                    UPDATE erp_products 
                    SET current_stock = COALESCE(current_stock, 0) + %s 
                    WHERE id = %s
                """, (safe_float(item.get('quantity', 0)), product_id))

            cursor.execute("""
                UPDATE erp_purchase_orders 
                SET status = 'Received', approval_status = 'Approved'
                WHERE id = %s
            """, [po_id])

            conn.commit()
            cursor.close()
            conn.close()

            return jsonify({
                "status": "success",
                "receipt_number": receipt_number,
                "total_quantity": safe_float(total_quantity),
                "total_cost": safe_float(total_cost),
                "items_received": len(items)
            }), 200

        except Exception as e:
            conn.rollback()
            logger.error(f"Goods receipt failed: {e}")
            return jsonify({"error": str(e)}), 500

    except Exception as e:
        logger.error(f"Goods receipt error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# RECENT SALES
# ============================================

@app.route('/recent', methods=['GET'])
def get_recent_sales():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        query = """
            SELECT 
                so.so_number as sale_id,
                c.customer_name,
                so.order_date as sale_date,
                TO_CHAR(so.order_time, 'HH24:MI:SS') as sale_time,
                so.total_amount as total_sales,
                COALESCE(so.rewards_earned, 0) as rewards_earned,
                so.status,
                so.approval_status,
                so.created_by as recorded_by,
                COALESCE(c.rewards_balance, 0) as rewards_balance,
                1 as etl_processed
            FROM erp_sales_orders so
            LEFT JOIN erp_customers c ON so.customer_id = c.id
            ORDER BY so.created_at DESC
            LIMIT 50
        """

        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()

        return jsonify(rows), 200

    except Exception as e:
        logger.error(f"Recent sales error: {e}")
        return jsonify([]), 200

# ============================================
# DYNAMIC CASH BALANCE
# ============================================

@app.route('/dynamic-cash-balance', methods=['GET'])
def get_dynamic_cash_balance():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"cash_balance": 0, "available_cash": 0}), 200

        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COALESCE(SUM(cash_balance), 0) FROM erp_bank_accounts")
            cash_balance = cursor.fetchone()[0] or 0
        except:
            cursor.execute("SELECT COALESCE(SUM(balance), 0) FROM erp_bank_accounts")
            cash_balance = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COALESCE(SUM(total_amount), 0) FROM erp_purchase_orders WHERE status NOT IN ('Received', 'Cancelled')")
        pending_purchases = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COALESCE(SUM(amount), 0) FROM erp_payments WHERE status = 'Pending'")
        pending_payments = cursor.fetchone()[0] or 0

        available_cash = safe_float(cash_balance) - safe_float(pending_purchases) - safe_float(pending_payments)

        cursor.close()
        conn.close()

        return jsonify({
            "cash_balance": safe_float(cash_balance),
            "pending_purchases": safe_float(pending_purchases),
            "pending_payments": safe_float(pending_payments),
            "available_cash": safe_float(available_cash)
        }), 200

    except Exception as e:
        logger.error(f"Cash balance error: {e}")
        return jsonify({"cash_balance": 0, "available_cash": 0}), 200

# ============================================
# BANK BALANCE
# ============================================

@app.route('/bank-balance', methods=['GET'])
def get_bank_balance():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"total_balance": 0}), 200

        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COALESCE(SUM(cash_balance), 0) FROM erp_bank_accounts")
            total_balance = cursor.fetchone()[0] or 0
        except:
            cursor.execute("SELECT COALESCE(SUM(balance), 0) FROM erp_bank_accounts")
            total_balance = cursor.fetchone()[0] or 0
        cursor.close()
        conn.close()
        return jsonify({"total_balance": safe_float(total_balance)}), 200

    except Exception as e:
        return jsonify({"total_balance": 0}), 200

# ============================================
# CUSTOMERS
# ============================================

@app.route('/customers', methods=['GET'])
def get_customers():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([]), 200

        query = """
            SELECT id, customer_code, customer_name, customer_type, email, phone, 
                   rewards_balance, is_active
            FROM erp_customers WHERE is_active = 1 ORDER BY customer_name
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()
        return jsonify(rows), 200

    except Exception as e:
        logger.error(f"Customers error: {e}")
        return jsonify([]), 200

# ============================================
# BANK ACCOUNTS
# ============================================

@app.route('/bank-accounts', methods=['GET'])
def get_bank_accounts():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify([]), 200

        query = """
            SELECT id, account_name, account_number, bank_name, balance, currency, is_active
            FROM erp_bank_accounts WHERE is_active = 1 ORDER BY account_name
        """
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(convert_row_to_serializable(row, columns))
        cursor.close()
        conn.close()
        return jsonify(rows), 200

    except Exception as e:
        logger.error(f"Bank accounts error: {e}")
        return jsonify([]), 200

# ============================================
# OVERDUE POS
# ============================================

@app.route('/overdue-pos', methods=['GET'])
def get_overdue_pos():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"overdue_count": 0}), 200

        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as overdue_count
            FROM erp_purchase_orders
            WHERE expected_delivery_date < CURRENT_DATE
            AND status NOT IN ('Received', 'Cancelled')
        """)
        count = cursor.fetchone()[0] or 0
        cursor.close()
        conn.close()
        return jsonify({"overdue_count": count}), 200

    except Exception as e:
        logger.error(f"Overdue POS error: {e}")
        return jsonify({"overdue_count": 0}), 200

# ============================================
# INCOMING DOCUMENTS
# ============================================

@app.route('/incoming-documents', methods=['GET'])
def get_incoming_documents():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"count": 0}), 200

        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM erp_documents
            WHERE status = 'Pending'
            AND is_active = 1
        """)
        count = cursor.fetchone()[0] or 0
        cursor.close()
        conn.close()
        return jsonify({"count": count}), 200

    except Exception as e:
        logger.error(f"Incoming documents error: {e}")
        return jsonify({"count": 0}), 200

# ============================================
# PENDING APPROVALS
# ============================================

@app.route('/pending-approvals', methods=['GET'])
def get_pending_approvals():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"pending_pos": 0, "pending_sos": 0}), 200

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM erp_purchase_orders WHERE approval_status = 'Pending'")
        pending_pos = cursor.fetchone()[0] or 0
        cursor.execute("SELECT COUNT(*) FROM erp_sales_orders WHERE approval_status = 'Pending'")
        pending_sos = cursor.fetchone()[0] or 0
        cursor.close()
        conn.close()
        return jsonify({"pending_pos": pending_pos, "pending_sos": pending_sos}), 200

    except Exception as e:
        logger.error(f"Pending approvals error: {e}")
        return jsonify({"pending_pos": 0, "pending_sos": 0}), 200

# ============================================
# UNPROCESSED PAYMENTS
# ============================================

@app.route('/unprocessed-payments', methods=['GET'])
def get_unprocessed_payments():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"count": 0}), 200

        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM erp_payments WHERE status = 'Pending'")
        count = cursor.fetchone()[0] or 0
        cursor.close()
        conn.close()
        return jsonify({"count": count}), 200

    except Exception as e:
        logger.error(f"Unprocessed payments error: {e}")
        return jsonify({"count": 0}), 200

# ============================================
# RECEIPT
# ============================================

@app.route('/receipt/<order_number>', methods=['GET'])
def generate_receipt(order_number):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        
        order_query = """
            SELECT so.so_number, c.customer_name, so.order_date, so.order_time,
                   so.total_amount, so.rewards_earned
            FROM erp_sales_orders so
            LEFT JOIN erp_customers c ON so.customer_id = c.id
            WHERE so.so_number = %s
        """
        cursor.execute(order_query, [order_number])
        order = cursor.fetchone()
        
        if not order:
            cursor.close()
            conn.close()
            return jsonify({"error": "Order not found"}), 404

        lines_query = """
            SELECT product_name, product_code, quantity, unit_price, line_total
            FROM erp_sales_order_lines
            WHERE so_id = (SELECT id FROM erp_sales_orders WHERE so_number = %s)
            ORDER BY line_number
        """
        cursor.execute(lines_query, [order_number])
        lines = cursor.fetchall()
        cursor.close()
        conn.close()

        receipt_data = {
            "order_number": order[0],
            "customer_name": order[1] or 'Unknown',
            "order_date": order[2].strftime('%Y-%m-%d') if order[2] else '',
            "order_time": order[3].strftime('%H:%M:%S') if order[3] else '',
            "total_amount": safe_float(order[4] or 0),
            "rewards_earned": safe_float(order[5] or 0),
            "lines": []
        }

        for line in lines:
            receipt_data["lines"].append({
                "product_name": line[0] or 'Unknown',
                "product_code": line[1] or '',
                "quantity": safe_float(line[2] or 0),
                "unit_price": safe_float(line[3] or 0),
                "line_total": safe_float(line[4] or 0)
            })

        return jsonify({"status": "success", "receipt": receipt_data}), 200

    except Exception as e:
        logger.error(f"Receipt error: {e}")
        return jsonify({"error": str(e)}), 500

# ============================================
# APPROVE / REJECT / DELETE PO
# ============================================

@app.route('/purchase-orders/<po_number>/approve', methods=['POST'])
def approve_purchase_order(po_number):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        cursor.execute("UPDATE erp_purchase_orders SET approval_status='Approved', status='Confirmed' WHERE po_number=%s", [po_number])
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "success", "message": f"PO {po_number} approved"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/purchase-orders/<po_number>/reject', methods=['POST'])
def reject_purchase_order(po_number):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        cursor.execute("UPDATE erp_purchase_orders SET approval_status='Rejected', status='Cancelled' WHERE po_number=%s", [po_number])
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "success", "message": f"PO {po_number} rejected"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/purchase-orders/<po_number>', methods=['DELETE'])
def delete_empty_purchase_order(po_number):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({"error": "Database connection failed"}), 500

        cursor = conn.cursor()
        cursor.execute("SELECT id FROM erp_purchase_orders WHERE po_number=%s", [po_number])
        po = cursor.fetchone()
        if not po:
            cursor.close()
            conn.close()
            return jsonify({"error": "PO not found"}), 404

        po_id = po[0]
        cursor.execute("SELECT COUNT(*) FROM erp_purchase_order_lines WHERE po_id=%s", [po_id])
        lines_count = cursor.fetchone()[0]

        if lines_count > 0:
            cursor.close()
            conn.close()
            return jsonify({"error": "Cannot delete PO with items"}), 400

        cursor.execute("DELETE FROM erp_purchase_orders WHERE id=%s", [po_id])
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"status": "success", "message": f"PO {po_number} deleted"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================
# CORS
# ============================================

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
    return response

# ============================================
# MAIN
# ============================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8000))
    print("=" * 70)
    print("🛒 SPAR ETL RECEIVER - SUPABASE VERSION")
    print("=" * 70)
    print(f"\n🚀 Starting on port {port}...")
    print(f"📡 Supabase Host: {SUPABASE_HOST}")
    print(f"📡 Database: {SUPABASE_DATABASE}")
    print("\n📍 Test URLs:")
    print(f"   http://localhost:{port}/")
    print(f"   http://localhost:{port}/health")
    print(f"   http://localhost:{port}/debug")
    print("=" * 70)
    print("\n⚠️ Make sure these environment variables are set:")
    print("   SUPABASE_HOST=db.livwipmybrvgtgrbtxkc.supabase.co")
    print("   SUPABASE_DATABASE=postgres")
    print("   SUPABASE_USERNAME=postgres")
    print("   SUPABASE_PASSWORD=W2QjDGkLDNOy87OC")
    print("   SUPABASE_PORT=5432")
    print("=" * 70)
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)