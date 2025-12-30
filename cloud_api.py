import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from sqlalchemy import create_engine, text
import json
import os
import urllib.parse
from dotenv import load_dotenv

# 1. Load the dynamic variables from .env file
load_dotenv()

app = FastAPI()

# 2. Construct the Safe DB URL
try:
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    # IMPORTANT: URL Encode the password to handle special characters safely
    encoded_password = urllib.parse.quote_plus(password)

    # Construct String: postgresql://user:encoded_pass@host:port/dbname?sslmode=require
    DB_URL = f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}?sslmode=require"
    
    # Create Engine
    engine = create_engine(DB_URL)
    
except Exception as e:
    print(f"❌ Configuration Error: {e}")
    print("Make sure you created the .env file!")
    exit()

# --- MODELS ---
class LicenseCheck(BaseModel):
    tenant_id: str
    license_key: str
    device_id: str

class BatchData(BaseModel):
    tenant_id: str
    device_id: str
    batch_id: str
    sales: List[Dict[str, Any]]

# --- ENDPOINTS ---

@app.post("/api/verify-license")
def verify(data: LicenseCheck):
    try:
        with engine.connect() as conn:
            # 1. Check Tenant
            sql = text("SELECT license_key FROM tenants WHERE tenant_id = :tid")
            result = conn.execute(sql, {"tid": data.tenant_id}).fetchone()
            
            if not result or result[0] != data.license_key:
                raise HTTPException(status_code=401, detail="Invalid License")

            # 2. Register/Update Device
            sql_dev = text("""
                INSERT INTO devices (device_id, tenant_id, last_seen)
                VALUES (:did, :tid, NOW())
                ON CONFLICT (device_id) DO UPDATE SET last_seen = NOW()
            """)
            conn.execute(sql_dev, {"did": data.device_id, "tid": data.tenant_id})
            conn.commit()
            
            return {"status": "active"}
    except Exception as e:
        print(f"Database Error: {e}")
        raise HTTPException(status_code=500, detail="Database Connection Failed")

@app.post("/api/sync/sales")
def sync_sales(data: BatchData):
    print(f"Processing Batch {data.batch_id}...")
    
    try:
        with engine.connect() as conn:
            # 1. Save Raw Batch
            conn.execute(text("""
                INSERT INTO sync_batches (batch_id, tenant_id, device_id, raw_data)
                VALUES (:bid, :tid, :did, :json)
            """), {
                "bid": data.batch_id,
                "tid": data.tenant_id,
                "did": data.device_id,
                "json": json.dumps(data.sales)
            })

            # 2. Process Sales into Report Table
            for sale in data.sales:
                total = float(sale.get('CalculatedTotal', 0))
                cog_total = 0
                
                for item in sale.get('items', []):
                    cog_total += float(item.get('COG', 0)) * float(item.get('Quantity', 0))
                
                profit = total - cog_total
                
                conn.execute(text("""
                    INSERT INTO sales_report 
                    (tenant_id, local_sale_id, sale_date, customer_name, cashier_name, total_amount, total_cog, profit, items_json)
                    VALUES (:tid, :lid, :date, :cust, :cash, :amt, :cog, :prof, :items)
                    ON CONFLICT (tenant_id, local_sale_id) DO NOTHING
                """), {
                    "tid": data.tenant_id,
                    "lid": sale['CashSalesID'],
                    "date": sale['Date'],
                    "cust": sale.get('Customer', 'Unknown'),
                    "cash": sale.get('Cashier', 'Unknown'),
                    "amt": total,
                    "cog": cog_total,
                    "prof": profit,
                    "items": json.dumps(sale['items'])
                })
            
            conn.commit()
        return {"status": "success"}
    except Exception as e:
        print(f"Sync Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)