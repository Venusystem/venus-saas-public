import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text
import json
import os
import urllib.parse
from dotenv import load_dotenv

# 1. Load Configuration
load_dotenv()

app = FastAPI()

# 2. Database Connection
try:
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if not password:
        print("⚠️ Warning: DB_PASSWORD not found in .env")

    encoded_password = urllib.parse.quote_plus(password) if password else ""
    DB_URL = f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}?sslmode=require"
    
    engine = create_engine(DB_URL)
    
except Exception as e:
    print(f"❌ Configuration Error: {e}")
    exit()

# --- DATA MODELS ---
class LicenseCheck(BaseModel):
    tenant_id: str
    license_key: str
    device_id: str

class BatchData(BaseModel):
    tenant_id: str
    device_id: str
    batch_id: str
    data: List[Dict[str, Any]] # Generic list (Works for Sales, Items, etc.)

# --- ROUTES ---

@app.get("/")
def root():
    return {"message": "Venus SaaS API v2 (ERP Ready) is Live!"}

@app.post("/api/verify-license")
def verify(data: LicenseCheck):
    try:
        with engine.connect() as conn:
            # Check Tenant & Subscription Status
            sql = text("SELECT license_key, subscription_status FROM tenants WHERE tenant_id = :tid")
            result = conn.execute(sql, {"tid": data.tenant_id}).fetchone()
            
            if not result:
                raise HTTPException(status_code=401, detail="Tenant ID not found")
            
            stored_key, status = result
            
            if stored_key != data.license_key:
                raise HTTPException(status_code=401, detail="Invalid License Key")
            
            if status != 'active':
                raise HTTPException(status_code=402, detail="Subscription Expired/Suspended")

            # Update Device Last Seen
            sql_dev = text("""
                INSERT INTO devices (device_id, tenant_id, last_seen)
                VALUES (:did, :tid, NOW())
                ON CONFLICT (device_id) DO UPDATE SET last_seen = NOW()
            """)
            conn.execute(sql_dev, {"did": data.device_id, "tid": data.tenant_id})
            conn.commit()
            
            return {"status": "active"}
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Auth Error: {e}")
        raise HTTPException(status_code=500, detail="Server Error")

# --- 1. SYNC ITEMS (Inventory) ---
@app.post("/api/sync/items")
def sync_items(batch: BatchData):
    print(f"📦 Syncing {len(batch.data)} Items...")
    with engine.connect() as conn:
        for item in batch.data:
            # We use ON CONFLICT DO UPDATE so price changes are reflected
            sql = text("""
                INSERT INTO items (id, tenant_id, local_id, name, code, category_name, 
                                   cost_price, selling_price, wholesale_price, opening_stock)
                VALUES (gen_random_uuid(), :tid, :lid, :name, :code, :cat, :cost, :price, :ws, :qty)
                ON CONFLICT (tenant_id, local_id) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    selling_price = EXCLUDED.selling_price,
                    cost_price = EXCLUDED.cost_price,
                    stock_quantity = EXCLUDED.opening_stock,
                    updated_at = NOW()
            """)
            conn.execute(sql, {
                "tid": batch.tenant_id,
                "lid": item['local_id'],
                "name": item.get('name'),
                "code": item.get('Code'),
                "cat": item.get('CategoryName'),
                "cost": item.get('CostPrice', 0),
                "price": item.get('SellingPrice', 0),
                "ws": item.get('wholesale_price', 0),
                "qty": item.get('OpeningStock', 0)
            })
        conn.commit()
    return {"status": "success"}

# --- 2. SYNC SALES ---
@app.post("/api/sync/sales")
def sync_sales(batch: BatchData):
    print(f"💰 Syncing {len(batch.data)} Sales...")
    with engine.connect() as conn:
        for sale in batch.data:
            # 1. Insert Header
            sql = text("""
                INSERT INTO sales (id, tenant_id, local_id, sale_date, sale_time, 
                                   store_name, customer_name, cashier_name, 
                                   total_amount, pay_mode, shift, sale_type)
                VALUES (gen_random_uuid(), :tid, :lid, :date, :time, 
                        :store, :cust, :cash, :amt, :pay, :shift, :type)
                ON CONFLICT (tenant_id, local_id) DO NOTHING
            """)
            conn.execute(sql, {
                "tid": batch.tenant_id,
                "lid": sale['local_id'],
                "date": sale['sale_date'],
                "time": sale.get('sale_time'),
                "store": sale.get('StoreName'),
                "cust": sale.get('CustomerName'),
                "cash": sale.get('cashier_name'),
                "amt": sale.get('TotalAmount', 0),
                "pay": sale.get('Paymode'),
                "shift": sale.get('Shift'),
                "type": sale.get('sale_type')
            })
            
            # 2. Insert Items (Only if not duplicate)
            # Note: A real production system might need more complex logic to update items
            for item in sale.get('items', []):
                sql_item = text("""
                    INSERT INTO sale_items (id, tenant_id, sale_local_id, item_name, quantity, price, total, cog)
                    VALUES (gen_random_uuid(), :tid, :slid, :name, :qty, :price, :total, :cog)
                """)
                # We check IF EXISTS to avoid double inserting items on re-sync
                # (Ideally, we'd use a unique constraint on items too, but this is safe for MVP)
                conn.execute(sql_item, {
                    "tid": batch.tenant_id,
                    "slid": sale['local_id'],
                    "name": str(item.get('ItemID', 'Unknown')), # Or lookup name if available
                    "qty": item.get('Quantity', 0),
                    "price": item.get('Price', 0),
                    "total": item.get('total', 0),
                    "cog": item.get('COG', 0)
                })

        conn.commit()
    return {"status": "success"}

# --- 3. SYNC PURCHASES ---
@app.post("/api/sync/purchases")
def sync_purchases(batch: BatchData):
    print(f"🚚 Syncing {len(batch.data)} Purchases...")
    with engine.connect() as conn:
        for purch in batch.data:
            sql = text("""
                INSERT INTO purchases (id, tenant_id, local_id, purchase_date, reference_no,
                                       store_name, supplier_name, purchase_type, total_amount)
                VALUES (gen_random_uuid(), :tid, :lid, :date, :ref, :store, :sup, :type, :amt)
                ON CONFLICT (tenant_id, local_id) DO NOTHING
            """)
            conn.execute(sql, {
                "tid": batch.tenant_id,
                "lid": purch['local_id'],
                "date": purch['purchase_date'],
                "ref": purch.get('Reference'),
                "store": purch.get('StoreName'),
                "sup": purch.get('SupplierName'),
                "type": purch.get('purchase_type'),
                "amt": purch.get('total_amount', 0)
            })

            # Items
            for item in purch.get('items', []):
                conn.execute(text("""
                    INSERT INTO purchase_items (id, tenant_id, purchase_local_id, item_name, quantity, price, total)
                    VALUES (gen_random_uuid(), :tid, :plid, :name, :qty, :price, :total)
                """), {
                    "tid": batch.tenant_id,
                    "plid": purch['local_id'],
                    "name": str(item.get('ItemID', 'Unknown')),
                    "qty": item.get('Quantity', 0),
                    "price": item.get('Price', 0),
                    "total": item.get('total', 0)
                })
        conn.commit()
    return {"status": "success"}

# --- 4. SYNC EXPENSES ---
@app.post("/api/sync/expenses")
def sync_expenses(batch: BatchData):
    print(f"💸 Syncing {len(batch.data)} Expenses...")
    with engine.connect() as conn:
        for exp in batch.data:
            sql = text("""
                INSERT INTO expenses (id, tenant_id, local_id, expense_date, reference,
                                      store_name, supplier_name, cashier_name, total_amount)
                VALUES (gen_random_uuid(), :tid, :lid, :date, :ref, :store, :payee, :cash, :amt)
                ON CONFLICT (tenant_id, local_id) DO NOTHING
            """)
            conn.execute(sql, {
                "tid": batch.tenant_id,
                "lid": exp['local_id'],
                "date": exp['expense_date'],
                "ref": exp.get('Reference'),
                "store": exp.get('StoreName'),
                "payee": exp.get('payee'),
                "cash": exp.get('cashier'),
                "amt": exp.get('total_amount', 0)
            })

            # Details
            for item in exp.get('items', []):
                conn.execute(text("""
                    INSERT INTO expense_details (id, tenant_id, expense_local_id, account_name, description, quantity, price, total)
                    VALUES (gen_random_uuid(), :tid, :elid, :cat, :desc, :qty, :price, :total)
                """), {
                    "tid": batch.tenant_id,
                    "elid": exp['local_id'],
                    "cat": item.get('category'),
                    "desc": item.get('Description'),
                    "qty": item.get('Quantity', 0),
                    "price": item.get('Price', 0),
                    "total": (item.get('Price', 0) * item.get('Quantity', 0))
                })
        conn.commit()
    return {"status": "success"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)