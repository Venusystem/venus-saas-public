import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
import json
import os
import urllib.parse
from dotenv import load_dotenv

load_dotenv()
app = FastAPI()

try:
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    encoded_pw = urllib.parse.quote_plus(password) if password else ""
    DB_URL = f"postgresql://{user}:{encoded_pw}@{host}:{port}/{db_name}?sslmode=require"
    engine = create_engine(DB_URL)
except Exception as e:
    print(f"Config Error: {e}")

class Payload(BaseModel):
    tenant_id: str
    license_key: str
    device_id: str
    data: List[Dict[str, Any]] = []

@app.get("/")
def root(): return {"message": "Venus ERP Sync v6 (Fixed Params)"}

# --- AUTH ---
@app.post("/api/verify-license")
def verify(p: Payload):
    with engine.begin() as conn:
        res = conn.execute(text("SELECT subscription_status FROM tenants WHERE tenant_id=:t AND license_key=:k"), 
                           {"t": p.tenant_id, "k": p.license_key}).fetchone()
        if not res or res[0] != 'active': raise HTTPException(401, "Invalid/Expired")
        conn.execute(text("INSERT INTO devices (device_id, tenant_id, last_seen) VALUES (:d, :t, NOW()) ON CONFLICT (device_id) DO UPDATE SET last_seen=NOW()"), {"d": p.device_id, "t": p.tenant_id})
    return {"status": "active"}

# --- ITEMS ---
@app.post("/api/sync/items")
def sync_items(p: Payload):
    try:
        with engine.begin() as conn:
            for r in p.data:
                conn.execute(text("""
                    INSERT INTO items (id, tenant_id, local_id, name, code, category, cost_price, selling_price, wholesale_price, opening_stock)
                    VALUES (gen_random_uuid(), :tid, :lid, :name, :code, :cat, :cost, :price, :wholesale, :stock)
                    ON CONFLICT (tenant_id, local_id) DO UPDATE SET 
                        name=EXCLUDED.name, selling_price=EXCLUDED.selling_price, opening_stock=EXCLUDED.opening_stock, cost_price=EXCLUDED.cost_price
                """), {
                    "tid": p.tenant_id, "lid": r['local_id'], "name": r['name'], "code": r['code'],
                    "cat": r['category'], "cost": r['cost'], "price": r['price'], "wholesale": r['wholesale'], "stock": r['stock']
                })
        return {"status": "success"}
    except Exception as e:
        print(f"Item Error: {e}")
        raise HTTPException(500, str(e))

# --- SALES (FIXED) ---
@app.post("/api/sync/sales")
def sync_sales(p: Payload):
    try:
        with engine.begin() as conn:
            for r in p.data:
                # Header
                conn.execute(text("""
                    INSERT INTO sales (id, tenant_id, local_id, sale_date, store_name, customer_name, cashier_name, pay_mode, total_amount)
                    VALUES (gen_random_uuid(), :tid, :lid, :date, :store, :cust, :cash, :pay, :total)
                    ON CONFLICT (tenant_id, local_id) DO NOTHING
                """), {
                    "tid": p.tenant_id, "lid": r['local_id'], "date": r['date'],
                    "store": r['store'], "cust": r['customer'], "cash": r['cashier'],
                    "pay": r['paymode'], "total": r['total']
                })
                
                # Delete old items to prevent duplicates
                conn.execute(text("DELETE FROM sale_items WHERE tenant_id=:tid AND sale_local_id=:sid"), {"tid": p.tenant_id, "sid": r['local_id']})

                # Details (Fixed Parameters match JSON keys)
                for i in r.get('items', []):
                    conn.execute(text("""
                        INSERT INTO sale_items (id, tenant_id, sale_local_id, item_name, quantity, price, total, cog)
                        VALUES (gen_random_uuid(), :tid, :sid, :name, :qty, :price, :total, :cog)
                    """), {
                        "tid": p.tenant_id, "sid": r['local_id'],
                        "name": i['name'], "qty": i['qty'], "price": i['price'],
                        "total": i['total'], "cog": i['cog']
                    })
        return {"status": "success"}
    except Exception as e:
        print(f"Sales Error: {e}")
        raise HTTPException(500, str(e))

# --- PURCHASES (FIXED) ---
@app.post("/api/sync/purchases")
def sync_purchases(p: Payload):
    try:
        with engine.begin() as conn:
            for r in p.data:
                conn.execute(text("""
                    INSERT INTO purchases (id, tenant_id, local_id, purchase_date, reference, store_name, supplier_name, total_amount)
                    VALUES (gen_random_uuid(), :tid, :lid, :date, :ref, :store, :sup, :total)
                    ON CONFLICT (tenant_id, local_id) DO NOTHING
                """), {
                    "tid": p.tenant_id, "lid": r['local_id'], "date": r['date'], "ref": r['reference'], 
                    "store": r['store'], "sup": r['supplier'], "total": r['total']
                })
                
                conn.execute(text("DELETE FROM purchase_items WHERE tenant_id=:tid AND purchase_local_id=:pid"), {"tid": p.tenant_id, "pid": r['local_id']})

                for i in r.get('items', []):
                    conn.execute(text("""
                        INSERT INTO purchase_items (id, tenant_id, purchase_local_id, item_name, quantity, price, total)
                        VALUES (gen_random_uuid(), :tid, :pid, :name, :qty, :price, :total)
                    """), {
                        "tid": p.tenant_id, "pid": r['local_id'], "name": i['name'],
                        "qty": i['qty'], "price": i['price'], "total": i['total']
                    })
        return {"status": "success"}
    except Exception as e:
        print(f"Purchase Error: {e}")
        raise HTTPException(500, str(e))

# --- EXPENSES (FIXED) ---
@app.post("/api/sync/expenses")
def sync_expenses(p: Payload):
    try:
        with engine.begin() as conn:
            for r in p.data:
                conn.execute(text("""
                    INSERT INTO expenses (id, tenant_id, local_id, expense_date, reference, store_name, payee_name, cashier_name, total_amount)
                    VALUES (gen_random_uuid(), :tid, :lid, :date, :ref, :store, :payee, :cashier, :total)
                    ON CONFLICT (tenant_id, local_id) DO NOTHING
                """), {
                    "tid": p.tenant_id, "lid": r['local_id'], "date": r['date'], "ref": r['reference'], 
                    "store": r['store'], "payee": r['payee'], "cashier": r['cashier'], "total": r['total']
                })
                
                conn.execute(text("DELETE FROM expense_items WHERE tenant_id=:tid AND expense_local_id=:eid"), {"tid": p.tenant_id, "eid": r['local_id']})

                for i in r.get('items', []):
                    conn.execute(text("""
                        INSERT INTO expense_items (id, tenant_id, expense_local_id, category, description, amount)
                        VALUES (gen_random_uuid(), :tid, :eid, :cat, :desc, :amount)
                    """), {
                        "tid": p.tenant_id, "eid": r['local_id'], "cat": i['category'],
                        "desc": i['description'], "amount": i['amount']
                    })
        return {"status": "success"}
    except Exception as e:
        print(f"Expense Error: {e}")
        raise HTTPException(500, str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)
