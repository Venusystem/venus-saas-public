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
        print("❌ Configuration Error: DB_PASSWORD not found in .env")
        exit()
    if not host:
        print("❌ Configuration Error: DB_HOST not found in .env")
        exit()

    encoded_password = urllib.parse.quote_plus(password)
    DB_URL = f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}?sslmode=require"
    
    engine = create_engine(DB_URL)
    
except Exception as e:
    print(f"❌ Configuration Error: {e}")
    exit()

# --- DATA MODELS ---
# These models describe the shape of the data the API expects.
# If the incoming data doesn't match, FastAPI will return a 422 error (Validation Error).

class ItemBase(BaseModel):
    local_id: int
    name: str
    code: Optional[str] = None
    cost_price: float = 0.0
    selling_price: float = 0.0
    wholesale_price: float = 0.0
    opening_stock: float = 0.0
    category_name: Optional[str] = "General"

class SaleItem(BaseModel):
    ItemID: Optional[str] # Changed from local_id to ItemID as per your Access query
    Quantity: float = 0.0
    Price: float = 0.0
    COG: float = 0.0
    total: float = 0.0

class SaleHeader(BaseModel):
    local_id: int
    sale_date: str
    sale_time: Optional[str] = None
    TotalAmount: float = 0.0
    Paymode: Optional[str] = None
    Shift: Optional[str] = None
    sale_type: Optional[str] = None
    StoreName: Optional[str] = "Unknown Store"
    CustomerName: Optional[str] = "Walk-in"
    cashier_name: Optional[str] = "Unknown"
    items: List[SaleItem]

class PurchaseItem(BaseModel):
    ItemID: Optional[str]
    Quantity: float = 0.0
    Price: float = 0.0
    total: float = 0.0

class PurchaseHeader(BaseModel):
    local_id: int
    purchase_date: str
    Reference: Optional[str] = None
    purchase_type: Optional[str] = None
    StoreName: Optional[str] = "Unknown Store"
    SupplierName: Optional[str] = "Unknown Supplier"
    items: List[PurchaseItem]
    total_amount: float = 0.0

class ExpenseItem(BaseModel):
    Quantity: float = 0.0
    Price: float = 0.0
    Description: Optional[str] = None
    category: Optional[str] = "General"

class ExpenseHeader(BaseModel):
    local_id: int
    expense_date: str
    Reference: Optional[str] = None
    StoreName: Optional[str] = "Unknown Store"
    payee: Optional[str] = "Unknown"
    cashier: Optional[str] = "Unknown"
    items: List[ExpenseItem]
    total_amount: float = 0.0

# --- BATCH MODELS ---
# Wrapper for incoming data batches
class ItemBatch(BaseModel):
    tenant_id: str
    device_id: str
    batch_id: str
    data: List[ItemBase]

class SalesBatch(BaseModel):
    tenant_id: str
    device_id: str
    batch_id: str
    sales: List[SaleHeader] # List of Sale Headers, each containing its items

class PurchasesBatch(BaseModel):
    tenant_id: str
    device_id: str
    batch_id: str
    data: List[PurchaseHeader]

class ExpensesBatch(BaseModel):
    tenant_id: str
    device_id: str
    batch_id: str
    data: List[ExpenseHeader]

# --- API ENDPOINTS ---

@app.get("/")
def root():
    return {"message": "Venus SaaS API v2 (ERP Ready) is Live!"}

@app.post("/api/verify-license")
def verify(data: LicenseCheck):
    try:
        with engine.connect() as conn:
            sql = text("SELECT license_key, subscription_status FROM tenants WHERE tenant_id = :tid")
            result = conn.execute(sql, {"tid": data.tenant_id}).fetchone()
            
            if not result:
                raise HTTPException(status_code=401, detail="Tenant not found")
            
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
        raise HTTPException(status_code=500, detail="Server Error during verification")

# --- SYNC ENDPOINTS ---

@app.post("/api/sync/items")
def sync_items(batch: ItemBatch):
    try:
        with engine.connect() as conn:
            for item in batch.data:
                # Use item.local_id to get the actual ID from MS Access
                # Make sure the column names match EXACTLY what Python sent.
                # Sanitize data for database insertion
                sql = text("""
                    INSERT INTO items (id, tenant_id, local_id, name, code, category_name, 
                                       cost_price, selling_price, wholesale_price, opening_stock)
                    VALUES (gen_random_uuid(), :tid, :lid, :name, :code, :cat, :cost, :price, :ws, :qty)
                    ON CONFLICT (tenant_id, local_id) 
                    DO UPDATE SET 
                        name = EXCLUDED.name,
                        selling_price = EXCLUDED.selling_price,
                        cost_price = EXCLUDED.cost_price,
                        opening_stock = EXCLUDED.opening_stock,
                        category_name = EXCLUDED.category_name,
                        code = EXCLUDED.code,
                        updated_at = NOW()
                """)
                conn.execute(sql, {
                    "tid": batch.tenant_id,
                    "lid": item.local_id,
                    "name": item.name or "Unknown Item", # Ensure not null
                    "code": item.code or "",
                    "cat": item.category_name or "General",
                    "cost": item.cost_price,
                    "price": item.selling_price,
                    "ws": item.wholesale_price,
                    "qty": item.opening_stock
                })
            conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"API Items Sync Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error - Check Item Data")

@app.post("/api/sync/sales")
def sync_sales(batch: SalesBatch):
    try:
        with engine.connect() as conn:
            for sale in batch.sales:
                # 1. Insert Header
                sql_header = text("""
                    INSERT INTO sales (id, tenant_id, local_id, sale_date, sale_time, 
                                       store_name, customer_name, cashier_name, 
                                       total_amount, pay_mode, shift, sale_type)
                    VALUES (gen_random_uuid(), :tid, :lid, :date, :time, 
                            :store, :cust, :cash, :amt, :pay, :shift, :type)
                    ON CONFLICT (tenant_id, local_id) DO NOTHING
                """)
                conn.execute(sql_header, {
                    "tid": batch.tenant_id,
                    "lid": sale.local_id,
                    "date": sale.sale_date,
                    "time": sale.sale_time,
                    "store": sale.StoreName,
                    "cust": sale.CustomerName,
                    "cash": sale.cashier_name,
                    "amt": sale.TotalAmount,
                    "pay": sale.Paymode,
                    "shift": sale.Shift,
                    "type": sale.sale_type
                })
                
                # 2. Insert Items
                for item in sale.items:
                    sql_item = text("""
                        INSERT INTO sale_items (id, tenant_id, sale_local_id, item_name, quantity, price, total, cog)
                        VALUES (gen_random_uuid(), :tid, :slid, :name, :qty, :price, :total, :cog)
                    """)
                    conn.execute(sql_item, {
                        "tid": batch.tenant_id,
                        "slid": sale.local_id,
                        "name": item.ItemID or "Unknown Item", # Use ItemID from Access
                        "qty": item.Quantity,
                        "price": item.Price,
                        "total": item.total,
                        "cog": item.COG
                    })
            conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"API Sales Sync Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error - Check Sale Data")

@app.post("/api/sync/purchases")
def sync_purchases(batch: PurchasesBatch):
    try:
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
                    "lid": purch.local_id,
                    "date": purch.purchase_date,
                    "ref": purch.Reference,
                    "store": purch.StoreName,
                    "sup": purch.SupplierName,
                    "type": purch.purchase_type,
                    "amt": purch.total_amount
                })

                for item in purch.items:
                    conn.execute(text("""
                        INSERT INTO purchase_items (id, tenant_id, purchase_local_id, item_name, quantity, price, total)
                        VALUES (gen_random_uuid(), :tid, :plid, :name, :qty, :price, :total)
                    """), {
                        "tid": batch.tenant_id,
                        "plid": purch.local_id,
                        "name": item.ItemID or "Unknown Item",
                        "qty": item.Quantity,
                        "price": item.Price,
                        "total": item.total
                    })
            conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"API Purchases Sync Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error - Check Purchase Data")

@app.post("/api/sync/expenses")
def sync_expenses(batch: ExpensesBatch):
    try:
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
                    "lid": exp.local_id,
                    "date": exp.expense_date,
                    "ref": exp.Reference,
                    "store": exp.StoreName,
                    "payee": exp.payee,
                    "cash": exp.cashier,
                    "amt": exp.total_amount
                })

                for item in exp.items:
                    conn.execute(text("""
                        INSERT INTO expense_details (id, tenant_id, expense_local_id, account_name, description, quantity, price, total)
                        VALUES (gen_random_uuid(), :tid, :elid, :cat, :desc, :qty, :price, :total)
                    """), {
                        "tid": batch.tenant_id,
                        "elid": exp.local_id,
                        "cat": item.category or "General",
                        "desc": item.Description or "",
                        "qty": item.Quantity,
                        "price": item.Price,
                        "total": (item.Price or 0) * (item.Quantity or 0)
                    })
            conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"API Expenses Sync Error: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error - Check Expense Data")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)