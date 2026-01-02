import uvicorn
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, Column, String, DateTime, Float, UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import json
import os
import urllib.parse
from dotenv import load_dotenv
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
app = FastAPI(title="Venus ERP Sync API", version="4.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB Config - Fixed error handling
DB_URL = None
engine = None
SessionLocal = None

try:
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "venus_erp")
    
    if not all([user, host, port, db_name]):
        raise ValueError("Missing required database configuration")
    
    # Properly encode password
    encoded_pw = urllib.parse.quote_plus(password) if password else ""
    DB_URL = f"postgresql://{user}:{encoded_pw}@{host}:{port}/{db_name}"
    
    # Check for SSL mode
    sslmode = os.getenv("DB_SSLMODE", "prefer")
    if sslmode != "disable":
        DB_URL += f"?sslmode={sslmode}"
    
    logger.info(f"Connecting to database at {host}:{port}/{db_name}")
    
    engine = create_engine(DB_URL, pool_size=20, max_overflow=0, pool_pre_ping=True)
    
    # Test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        logger.info("Database connection successful")
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
except Exception as e:
    logger.error(f"Database configuration error: {e}")
    # Don't exit, allow the app to start but sync endpoints will fail gracefully

Base = declarative_base()

# Database dependency
def get_db():
    if SessionLocal is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not configured"
        )
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class Payload(BaseModel):
    tenant_id: str
    license_key: str
    device_id: str
    data: List[Dict[str, Any]] = []
    batch_id: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    version: str
    database: bool
    timestamp: datetime

@app.get("/")
def root():
    return {
        "message": "Venus ERP Sync v4 Live",
        "status": "operational",
        "endpoints": [
            "/api/verify-license",
            "/api/sync/items",
            "/api/sync/sales",
            "/api/sync/purchases",
            "/api/sync/expenses"
        ]
    }

@app.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_db)):
    db_status = False
    try:
        db.execute(text("SELECT 1"))
        db_status = True
    except:
        db_status = False
    
    return HealthResponse(
        status="healthy" if db_status else "degraded",
        version="4.0",
        database=db_status,
        timestamp=datetime.now(timezone.utc)
    )

# --- AUTH ---
@app.post("/api/verify-license")
def verify(p: Payload, db: Session = Depends(get_db)):
    logger.info(f"License verification request for tenant: {p.tenant_id}")
    
    try:
        # Check if tenant exists and license is valid
        result = db.execute(
            text("""
                SELECT subscription_status, company_name 
                FROM tenants 
                WHERE tenant_id = :tenant_id AND license_key = :license_key
            """),
            {"tenant_id": p.tenant_id, "license_key": p.license_key}
        ).fetchone()
        
        if not result:
            logger.warning(f"License not found for tenant: {p.tenant_id}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid tenant ID or license key"
            )
        
        subscription_status = result[0]
        company_name = result[1]
        
        if subscription_status != 'active':
            logger.warning(f"Inactive subscription for tenant: {p.tenant_id}")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Subscription {subscription_status}. Please renew your subscription."
            )
        
        # Update or insert device info
        db.execute(
            text("""
                INSERT INTO devices (device_id, tenant_id, last_seen) 
                VALUES (:device_id, :tenant_id, NOW())
                ON CONFLICT (device_id) 
                DO UPDATE SET last_seen = NOW(), tenant_id = EXCLUDED.tenant_id
            """),
            {"device_id": p.device_id, "tenant_id": p.tenant_id}
        )
        
        db.commit()
        logger.info(f"License verified successfully for tenant: {p.tenant_id}, device: {p.device_id}")
        
        return {
            "status": "active",
            "tenant_id": p.tenant_id,
            "company_name": company_name,
            "device_id": p.device_id,
            "message": "License verified successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"License verification error: {e}")
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during license verification"
        )

# --- SYNC ENDPOINTS ---

@app.post("/api/sync/items")
def sync_items(p: Payload, db: Session = Depends(get_db)):
    logger.info(f"Items sync request for tenant: {p.tenant_id}, records: {len(p.data)}")
    
    # Verify license first
    verify(p, db)
    
    if not p.data:
        return {"status": "success", "message": "No data to sync", "count": 0}
    
    try:
        success_count = 0
        errors = []
        
        for record in p.data:
            try:
                # Validate required fields
                required_fields = ['local_id', 'name']
                for field in required_fields:
                    if field not in record:
                        raise ValueError(f"Missing required field: {field}")
                
                db.execute(
                    text("""
                        INSERT INTO items (
                            tenant_id, local_id, name, code, category, 
                            cost_price, selling_price, wholesale_price, opening_stock
                        ) VALUES (
                            :tenant_id, :local_id, :name, :code, :category,
                            :cost_price, :selling_price, :wholesale_price, :opening_stock
                        )
                        ON CONFLICT (tenant_id, local_id) 
                        DO UPDATE SET 
                            name = EXCLUDED.name,
                            code = EXCLUDED.code,
                            category = EXCLUDED.category,
                            cost_price = EXCLUDED.cost_price,
                            selling_price = EXCLUDED.selling_price,
                            wholesale_price = EXCLUDED.wholesale_price,
                            opening_stock = EXCLUDED.opening_stock,
                            updated_at = NOW()
                    """),
                    {
                        "tenant_id": p.tenant_id,
                        "local_id": record.get('local_id'),
                        "name": record.get('name', ''),
                        "code": record.get('code', ''),
                        "category": record.get('category', 'General'),
                        "cost_price": float(record.get('cost', 0.0)),
                        "selling_price": float(record.get('price', 0.0)),
                        "wholesale_price": float(record.get('wholesale', 0.0)),
                        "opening_stock": float(record.get('stock', 0.0))
                    }
                )
                success_count += 1
                
            except Exception as e:
                errors.append(f"Record {record.get('local_id', 'unknown')}: {str(e)}")
                continue
        
        db.commit()
        
        response = {
            "status": "success",
            "count": success_count,
            "errors_count": len(errors)
        }
        
        if errors:
            response["errors"] = errors[:10]  # Limit errors in response
            logger.warning(f"Items sync completed with {len(errors)} errors")
        else:
            logger.info(f"Items sync successful: {success_count} records")
        
        return response
        
    except Exception as e:
        db.rollback()
        logger.error(f"Items sync error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync items: {str(e)}"
        )

@app.post("/api/sync/sales")
def sync_sales(p: Payload, db: Session = Depends(get_db)):
    logger.info(f"Sales sync request for tenant: {p.tenant_id}, records: {len(p.data)}")
    
    # Verify license first
    verify(p, db)
    
    if not p.data:
        return {"status": "success", "message": "No data to sync", "count": 0}
    
    try:
        success_count = 0
        item_success_count = 0
        errors = []
        
        for record in p.data:
            try:
                # Validate required fields
                if 'local_id' not in record:
                    raise ValueError("Missing required field: local_id")
                
                # Parse date string to timestamp
                sale_date = None
                if 'date' in record and record['date']:
                    try:
                        # Try to parse the date string
                        sale_date = record['date']
                    except:
                        sale_date = datetime.now()
                else:
                    sale_date = datetime.now()
                
                # Insert sale header
                db.execute(
                    text("""
                        INSERT INTO sales (
                            tenant_id, local_id, sale_date, store_name, 
                            customer_name, cashier_name, pay_mode, total_amount
                        ) VALUES (
                            :tenant_id, :local_id, :sale_date, :store_name,
                            :customer_name, :cashier_name, :pay_mode, :total_amount
                        )
                        ON CONFLICT (tenant_id, local_id) 
                        DO NOTHING
                    """),
                    {
                        "tenant_id": p.tenant_id,
                        "local_id": record.get('local_id'),
                        "sale_date": sale_date,
                        "store_name": record.get('store', 'Unknown'),
                        "customer_name": record.get('customer', 'Walk-in'),
                        "cashier_name": record.get('cashier', ''),
                        "pay_mode": record.get('paymode', 'Cash'),
                        "total_amount": float(record.get('total', 0.0))
                    }
                )
                success_count += 1
                
                # Insert sale items if available
                items = record.get('items', [])
                for item in items:
                    try:
                        db.execute(
                            text("""
                                INSERT INTO sale_items (
                                    tenant_id, sale_local_id, item_name, 
                                    quantity, price, total, cog
                                ) VALUES (
                                    :tenant_id, :sale_local_id, :item_name,
                                    :quantity, :price, :total, :cog
                                )
                            """),
                            {
                                "tenant_id": p.tenant_id,
                                "sale_local_id": record.get('local_id'),
                                "item_name": item.get('name', 'Unknown Item'),
                                "quantity": float(item.get('qty', 0.0)),
                                "price": float(item.get('price', 0.0)),
                                "total": float(item.get('total', 0.0)),
                                "cog": float(item.get('cog', 0.0))
                            }
                        )
                        item_success_count += 1
                    except Exception as e:
                        errors.append(f"Sale {record.get('local_id')} item error: {str(e)}")
                        continue
                
            except Exception as e:
                errors.append(f"Sale {record.get('local_id', 'unknown')}: {str(e)}")
                continue
        
        db.commit()
        
        response = {
            "status": "success",
            "sales_count": success_count,
            "items_count": item_success_count,
            "errors_count": len(errors)
        }
        
        if errors:
            response["errors"] = errors[:10]
            logger.warning(f"Sales sync completed with {len(errors)} errors")
        else:
            logger.info(f"Sales sync successful: {success_count} sales, {item_success_count} items")
        
        return response
        
    except Exception as e:
        db.rollback()
        logger.error(f"Sales sync error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync sales: {str(e)}"
        )

@app.post("/api/sync/purchases")
def sync_purchases(p: Payload, db: Session = Depends(get_db)):
    logger.info(f"Purchases sync request for tenant: {p.tenant_id}, records: {len(p.data)}")
    
    # Verify license first
    verify(p, db)
    
    if not p.data:
        return {"status": "success", "message": "No data to sync", "count": 0}
    
    try:
        success_count = 0
        item_success_count = 0
        errors = []
        
        for record in p.data:
            try:
                # Validate required fields
                if 'local_id' not in record:
                    raise ValueError("Missing required field: local_id")
                
                # Parse date
                purchase_date = None
                if 'date' in record and record['date']:
                    try:
                        purchase_date = record['date']
                    except:
                        purchase_date = datetime.now()
                else:
                    purchase_date = datetime.now()
                
                # Insert purchase header
                db.execute(
                    text("""
                        INSERT INTO purchases (
                            tenant_id, local_id, purchase_date, reference,
                            store_name, supplier_name, total_amount
                        ) VALUES (
                            :tenant_id, :local_id, :purchase_date, :reference,
                            :store_name, :supplier_name, :total_amount
                        )
                        ON CONFLICT (tenant_id, local_id) 
                        DO NOTHING
                    """),
                    {
                        "tenant_id": p.tenant_id,
                        "local_id": record.get('local_id'),
                        "purchase_date": purchase_date,
                        "reference": record.get('reference', ''),
                        "store_name": record.get('store', 'Unknown'),
                        "supplier_name": record.get('supplier', 'Unknown'),
                        "total_amount": float(record.get('total', 0.0))
                    }
                )
                success_count += 1
                
                # Insert purchase items
                items = record.get('items', [])
                for item in items:
                    try:
                        db.execute(
                            text("""
                                INSERT INTO purchase_items (
                                    tenant_id, purchase_local_id, item_name,
                                    quantity, price, total
                                ) VALUES (
                                    :tenant_id, :purchase_local_id, :item_name,
                                    :quantity, :price, :total
                                )
                            """),
                            {
                                "tenant_id": p.tenant_id,
                                "purchase_local_id": record.get('local_id'),
                                "item_name": item.get('name', 'Unknown Item'),
                                "quantity": float(item.get('qty', 0.0)),
                                "price": float(item.get('price', 0.0)),
                                "total": float(item.get('total', 0.0))
                            }
                        )
                        item_success_count += 1
                    except Exception as e:
                        errors.append(f"Purchase {record.get('local_id')} item error: {str(e)}")
                        continue
                
            except Exception as e:
                errors.append(f"Purchase {record.get('local_id', 'unknown')}: {str(e)}")
                continue
        
        db.commit()
        
        response = {
            "status": "success",
            "purchases_count": success_count,
            "items_count": item_success_count,
            "errors_count": len(errors)
        }
        
        if errors:
            response["errors"] = errors[:10]
            logger.warning(f"Purchases sync completed with {len(errors)} errors")
        else:
            logger.info(f"Purchases sync successful: {success_count} purchases, {item_success_count} items")
        
        return response
        
    except Exception as e:
        db.rollback()
        logger.error(f"Purchases sync error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync purchases: {str(e)}"
        )

@app.post("/api/sync/expenses")
def sync_expenses(p: Payload, db: Session = Depends(get_db)):
    logger.info(f"Expenses sync request for tenant: {p.tenant_id}, records: {len(p.data)}")
    
    # Verify license first
    verify(p, db)
    
    if not p.data:
        return {"status": "success", "message": "No data to sync", "count": 0}
    
    try:
        success_count = 0
        item_success_count = 0
        errors = []
        
        for record in p.data:
            try:
                # Validate required fields
                if 'local_id' not in record:
                    raise ValueError("Missing required field: local_id")
                
                # Parse date
                expense_date = None
                if 'date' in record and record['date']:
                    try:
                        expense_date = record['date']
                    except:
                        expense_date = datetime.now()
                else:
                    expense_date = datetime.now()
                
                # Insert expense header
                db.execute(
                    text("""
                        INSERT INTO expenses (
                            tenant_id, local_id, expense_date, reference,
                            store_name, payee_name, cashier_name, total_amount
                        ) VALUES (
                            :tenant_id, :local_id, :expense_date, :reference,
                            :store_name, :payee_name, :cashier_name, :total_amount
                        )
                        ON CONFLICT (tenant_id, local_id) 
                        DO NOTHING
                    """),
                    {
                        "tenant_id": p.tenant_id,
                        "local_id": record.get('local_id'),
                        "expense_date": expense_date,
                        "reference": record.get('reference', ''),
                        "store_name": record.get('store', 'Unknown'),
                        "payee_name": record.get('payee', ''),
                        "cashier_name": record.get('cashier', ''),
                        "total_amount": float(record.get('total', 0.0))
                    }
                )
                success_count += 1
                
                # Insert expense items
                items = record.get('items', [])
                for item in items:
                    try:
                        db.execute(
                            text("""
                                INSERT INTO expense_items (
                                    tenant_id, expense_local_id, category,
                                    description, amount
                                ) VALUES (
                                    :tenant_id, :expense_local_id, :category,
                                    :description, :amount
                                )
                            """),
                            {
                                "tenant_id": p.tenant_id,
                                "expense_local_id": record.get('local_id'),
                                "category": item.get('category', 'General'),
                                "description": item.get('description', ''),
                                "amount": float(item.get('amount', 0.0))
                            }
                        )
                        item_success_count += 1
                    except Exception as e:
                        errors.append(f"Expense {record.get('local_id')} item error: {str(e)}")
                        continue
                
            except Exception as e:
                errors.append(f"Expense {record.get('local_id', 'unknown')}: {str(e)}")
                continue
        
        db.commit()
        
        response = {
            "status": "success",
            "expenses_count": success_count,
            "items_count": item_success_count,
            "errors_count": len(errors)
        }
        
        if errors:
            response["errors"] = errors[:10]
            logger.warning(f"Expenses sync completed with {len(errors)} errors")
        else:
            logger.info(f"Expenses sync successful: {success_count} expenses, {item_success_count} items")
        
        return response
        
    except Exception as e:
        db.rollback()
        logger.error(f"Expenses sync error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync expenses: {str(e)}"
        )

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"},
    )

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Venus ERP Sync API")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=10000, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    
    args = parser.parse_args()
    
    logger.info(f"Starting Venus ERP Sync API on {args.host}:{args.port}")
    
    uvicorn.run(
        "cloud_api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )
