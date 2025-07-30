# production_api_system.py
import os
import json
import asyncio
import sqlite3
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import threading
from contextlib import contextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
import uvicorn
import requests
import hmac
import hashlib
import uuid

class SignalFileManager:
    """
    Manages your existing signal extractor output files:
    - tier1_customer_signals.json
    - tier2_customer_signals.json
    Runs your signal_extractor.py every 2 minutes
    """
    
    def __init__(self):
        self.tier1_file = "tier1_customer_signals.json"
        self.tier2_file = "tier2_customer_signals.json"
        self.signal_extractor_script = "signal_extractor.py"
        
        self.cached_tier1_data = {}
        self.cached_tier2_data = {}
        self.last_update = None
        self.extraction_running = False
        
    def start_background_loop(self):
        """Start background task to run signal extractor every 2 minutes"""
        def run_loop():
            while True:
                try:
                    if not self.extraction_running:
                        self.run_signal_extraction()
                    time.sleep(120)  # Wait 2 minutes
                except Exception as e:
                    print(f"❌ Background loop error: {e}")
                    time.sleep(60)  # Wait 1 minute on error
        
        # Start background thread
        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()
        print("🔄 Background signal extraction loop started (every 2 minutes)")
    
    def run_signal_extraction(self):
        """Run your existing signal_extractor.py script"""
        
        if self.extraction_running:
            print("⏭️ Extraction already running, skipping...")
            return
        
        try:
            self.extraction_running = True
            print("🚀 Running signal extractor...")
            
            # Run your existing signal extractor
            result = subprocess.run(
                ['python', self.signal_extractor_script],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print("✅ Signal extraction completed successfully")
                self.load_signal_files()
            else:
                print(f"❌ Signal extraction failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            print("⏰ Signal extraction timed out (5 minutes)")
        except Exception as e:
            print(f"❌ Error running signal extractor: {e}")
        finally:
            self.extraction_running = False
    
    def load_signal_files(self):
        """Load your generated JSON files into memory"""
        
        try:
            # Load Tier 1 data
            if os.path.exists(self.tier1_file):
                with open(self.tier1_file, 'r') as f:
                    self.cached_tier1_data = json.load(f)
                print(f"📊 Loaded Tier 1: {len(self.cached_tier1_data.get('signals', []))} signals")
            else:
                print(f"⚠️ Missing: {self.tier1_file}")
            
            # Load Tier 2 data  
            if os.path.exists(self.tier2_file):
                with open(self.tier2_file, 'r') as f:
                    self.cached_tier2_data = json.load(f)
                passed_count = len(self.cached_tier2_data.get('signals_passed', []))
                failed_count = len(self.cached_tier2_data.get('signals_failed', []))
                print(f"✅ Loaded Tier 2: {passed_count} passed, {failed_count} failed")
            else:
                print(f"⚠️ Missing: {self.tier2_file}")
            
            self.last_update = datetime.utcnow()
            print(f"🔄 Signal files loaded at {self.last_update.strftime('%H:%M:%S')}")
            
        except Exception as e:
            print(f"❌ Error loading signal files: {e}")
    
    def get_tier1_signals(self) -> Dict:
        """Get cached Tier 1 signals"""
        return self.cached_tier1_data
    
    def get_tier2_signals(self) -> Dict:
        """Get cached Tier 2 signals"""
        return self.cached_tier2_data
    
    def get_system_status(self) -> Dict:
        """Get system health status"""
        tier1_count = len(self.cached_tier1_data.get('signals', []))
        tier2_passed = len(self.cached_tier2_data.get('signals_passed', []))
        tier2_failed = len(self.cached_tier2_data.get('signals_failed', []))
        
        return {
            "signal_extractor_status": "running" if not self.extraction_running else "extracting",
            "last_update": self.last_update.isoformat() if self.last_update else None,
            "tier1_signals": tier1_count,
            "tier2_passed": tier2_passed,
            "tier2_failed": tier2_failed,
            "files_exist": {
                "tier1": os.path.exists(self.tier1_file),
                "tier2": os.path.exists(self.tier2_file)
            }
        }

class CustomerDatabase:
    """
    Customer management with automatic pricing transitions:
    - Launch pricing for first month
    - Automatic transition to regular pricing after 1 month
    """
    
    def __init__(self):
        self.pricing = {
            'basic': {'launch': 67, 'regular': 97},
            'pro': {'launch': 147, 'regular': 297}, 
            'enterprise': {'launch': 497, 'regular': 1500}
        }
        self._init_database()
    
    def _init_database(self):
        """Initialize customer database with pricing transition tracking"""
        with self._get_db() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS customers (
                    id TEXT PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    tier TEXT NOT NULL,
                    api_key TEXT UNIQUE,
                    subscription_status TEXT DEFAULT 'trial',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trial_ends_at TIMESTAMP,
                    launch_pricing_ends_at TIMESTAMP,
                    next_billing_date TIMESTAMP,
                    current_price REAL,
                    is_grandfathered BOOLEAN DEFAULT FALSE
                );
                
                CREATE TABLE IF NOT EXISTS payments (
                    id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    nowpayments_id TEXT UNIQUE,
                    amount_usd REAL,
                    pricing_type TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (customer_id) REFERENCES customers (id)
                );
                
                CREATE TABLE IF NOT EXISTS api_usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    customer_id TEXT,
                    endpoint TEXT,
                    requests_count INTEGER DEFAULT 1,
                    date DATE DEFAULT CURRENT_DATE,
                    FOREIGN KEY (customer_id) REFERENCES customers (id)
                );
            ''')
    
    @contextmanager
    def _get_db(self):
        conn = sqlite3.connect('production_customers.db')
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def get_customer_price(self, customer_id: str, tier: str) -> int:
        """Get current price for customer (launch vs regular pricing)"""
        
        with self._get_db() as conn:
            customer = conn.execute('''
                SELECT * FROM customers WHERE id = ?
            ''', (customer_id,)).fetchone()
            
            if not customer:
                # New customer gets launch pricing
                return self.pricing[tier]['launch']
            
            # Check if grandfathered at launch pricing
            if customer['is_grandfathered']:
                return customer['current_price']
            
            # Check if launch pricing period ended
            if customer['launch_pricing_ends_at']:
                launch_end = datetime.fromisoformat(customer['launch_pricing_ends_at'])
                if datetime.utcnow() > launch_end:
                    return self.pricing[tier]['regular']
            
            return self.pricing[tier]['launch']
    
    async def create_customer(self, email: str, tier: str) -> Dict:
        """Create new customer with 3-day trial + 1-month launch pricing"""
        
        customer_id = str(uuid.uuid4())
        current_price = self.pricing[tier]['launch']
        
        # 3-day trial + 1-month launch pricing
        trial_end = datetime.utcnow() + timedelta(days=3)
        launch_pricing_end = datetime.utcnow() + timedelta(days=33)  # 3 days trial + 30 days launch
        
        with self._get_db() as conn:
            conn.execute('''
                INSERT INTO customers 
                (id, email, tier, subscription_status, trial_ends_at, launch_pricing_ends_at, current_price)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (customer_id, email, tier, 'trial', trial_end, launch_pricing_end, current_price))
        
        return {
            "customer_id": customer_id,
            "email": email,
            "tier": tier,
            "current_price": current_price,
            "trial_ends_at": trial_end.isoformat(),
            "launch_pricing_ends_at": launch_pricing_end.isoformat(),
            "regular_price_starts": launch_pricing_end.isoformat()
        }
    
    async def activate_subscription(self, customer_id: str) -> Dict:
        """Activate customer subscription and generate API key"""
        
        api_key = f"as_{uuid.uuid4().hex[:24]}"
        next_billing = datetime.utcnow() + timedelta(days=30)
        
        with self._get_db() as conn:
            conn.execute('''
                UPDATE customers 
                SET subscription_status = 'active', api_key = ?, next_billing_date = ?
                WHERE id = ?
            ''', (api_key, next_billing, customer_id))
            
            customer = conn.execute('''
                SELECT * FROM customers WHERE id = ?
            ''', (customer_id,)).fetchone()
        
        return {
            "customer_id": customer_id,
            "api_key": api_key,
            "tier": customer['tier'],
            "current_price": customer['current_price'],
            "next_billing": next_billing.isoformat(),
            "status": "activated"
        }
    
    async def check_pricing_transitions(self):
        """Check for customers who need to transition to regular pricing"""
        
        with self._get_db() as conn:
            # Find customers whose launch pricing expired
            expired_customers = conn.execute('''
                SELECT * FROM customers 
                WHERE launch_pricing_ends_at < ? 
                AND is_grandfathered = FALSE
                AND subscription_status = 'active'
            ''', (datetime.utcnow().isoformat(),)).fetchall()
            
            for customer in expired_customers:
                new_price = self.pricing[customer['tier']]['regular']
                
                # Grandfather them at current price, send notification
                conn.execute('''
                    UPDATE customers 
                    SET is_grandfathered = TRUE,
                        current_price = ?
                    WHERE id = ?
                ''', (customer['current_price'], customer['id']))
                
                print(f"👑 Customer {customer['email']} grandfathered at ${customer['current_price']}/month")
                # TODO: Send email about grandfathering benefit
    
    async def verify_api_access(self, api_key: str) -> Dict:
        """Verify API key and return customer permissions"""
        
        with self._get_db() as conn:
            customer = conn.execute('''
                SELECT * FROM customers WHERE api_key = ?
            ''', (api_key,)).fetchone()
            
            if not customer:
                raise HTTPException(status_code=401, detail="Invalid API key")
            
            # Check trial expiration
            if customer['subscription_status'] == 'trial':
                trial_end = datetime.fromisoformat(customer['trial_ends_at'])
                if datetime.utcnow() > trial_end:
                    raise HTTPException(status_code=402, detail="Trial expired - payment required")
            
            # Log API usage
            conn.execute('''
                INSERT OR REPLACE INTO api_usage (customer_id, endpoint, requests_count, date)
                VALUES (?, ?, COALESCE((SELECT requests_count FROM api_usage 
                                     WHERE customer_id = ? AND endpoint = ? AND date = CURRENT_DATE), 0) + 1, CURRENT_DATE)
            ''', (customer['id'], 'api_access', customer['id'], 'api_access'))
            
            return {
                "customer_id": customer['id'],
                "tier": customer['tier'],
                "email": customer['email'],
                "current_price": customer['current_price'],
                "is_grandfathered": bool(customer['is_grandfathered']),
                "permissions": self._get_tier_permissions(customer['tier'])
            }
    
    def _get_tier_permissions(self, tier: str) -> Dict:
        """Get API access permissions for each tier"""
        
        permissions = {
            'basic': {
                'tier1_access': True,
                'tier2_access': False,
                'api_calls_per_minute': 60,
                'webhook_access': False,
                'priority_access': False
            },
            'pro': {
                'tier1_access': True,
                'tier2_access': True,
                'api_calls_per_minute': 300,
                'webhook_access': False,
                'priority_access': False
            },
            'enterprise': {
                'tier1_access': True,
                'tier2_access': True,
                'api_calls_per_minute': 1000,
                'webhook_access': True,
                'priority_access': True
            }
        }
        
        return permissions.get(tier, permissions['basic'])

class NOWPaymentsHandler:
    """NOWPayments integration for crypto subscriptions"""
    
    def __init__(self, customer_db: CustomerDatabase):
        self.api_key = os.getenv('NOWPAYMENTS_API_KEY')
        self.ipn_secret = os.getenv('NOWPAYMENTS_IPN_SECRET')
        self.customer_db = customer_db
    
    async def create_subscription_payment(self, email: str, tier: str, currency: str = 'usdterc20') -> Dict:
        """Create crypto payment for subscription"""
        
        # Create customer first
        customer_info = await self.customer_db.create_customer(email, tier)
        customer_id = customer_info['customer_id']
        price = customer_info['current_price']
        
        payment_data = {
            "price_amount": price,
            "price_currency": "usd",
            "pay_currency": currency,
            "order_id": f"{customer_id}_{tier}_{int(time.time())}",
            "order_description": f"{tier.title()} Signals - Monthly Subscription (Launch Price ${price})",
            "ipn_callback_url": "https://arbitrage-signals-landing-production.up.railway.app/webhooks/nowpayments",
            "customer_email": email
        }
        
        headers = {'x-api-key': self.api_key, 'Content-Type': 'application/json'}
        
        try:
            response = requests.post("https://api.nowpayments.io/v1/payment",
                                   headers=headers, json=payment_data)
            response.raise_for_status()
            payment = response.json()
            
            # Store payment record
            with self.customer_db._get_db() as conn:
                conn.execute('''
                    INSERT INTO payments (id, customer_id, nowpayments_id, amount_usd, pricing_type, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (str(uuid.uuid4()), customer_id, payment['payment_id'], 
                     price, 'launch', 'waiting'))
            
            return {
                "customer_id": customer_id,
                "payment_id": payment['payment_id'],
                "payment_address": payment['pay_address'],
                "amount_to_pay": payment['pay_amount'],
                "currency": payment['pay_currency'],
                "price_usd": price,
                "pricing_info": {
                    "current_price": price,
                    "regular_price": self.customer_db.pricing[tier]['regular'],
                    "launch_pricing_ends": customer_info['launch_pricing_ends_at']
                }
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Payment creation failed: {str(e)}")
    
    async def handle_payment_confirmation(self, payment_id: str) -> Dict:
        """Handle confirmed payment and activate subscription"""
        
        with self.customer_db._get_db() as conn:
            payment = conn.execute('''
                SELECT p.*, c.* FROM payments p 
                JOIN customers c ON p.customer_id = c.id 
                WHERE p.nowpayments_id = ?
            ''', (payment_id,)).fetchone()
            
            if not payment:
                raise HTTPException(status_code=404, detail="Payment not found")
            
            # Activate subscription
            activation_result = await self.customer_db.activate_subscription(payment['customer_id'])
            
            # Update payment status
            conn.execute('''
                UPDATE payments SET status = 'completed' WHERE nowpayments_id = ?
            ''', (payment_id,))
            
            return activation_result

# FastAPI Application
app = FastAPI(title="Professional Arbitrage Signals API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize systems
signal_manager = SignalFileManager()
customer_db = CustomerDatabase()
payments = NOWPaymentsHandler(customer_db)
security = HTTPBearer()

@app.on_event("startup")
async def startup_event():
    """Initialize signal extraction loop"""
    signal_manager.start_background_loop()
    signal_manager.run_signal_extraction()  # Run once immediately

@app.get("/")
async def root():
    """API status showing live signal data"""
    
    system_status = signal_manager.get_system_status()
    tier1_data = signal_manager.get_tier1_signals()
    tier2_data = signal_manager.get_tier2_signals()
    
    return {
        "status": "Professional Arbitrage Signals API - Live",
        "version": "2.0.0",
        "system": system_status,
        "live_signals": {
            "tier1_opportunities": system_status['tier1_signals'],
            "tier2_validated": system_status['tier2_passed'],
            "tier2_filtered": system_status['tier2_failed'],
            "success_rate": f"{(system_status['tier2_passed']/(system_status['tier1_signals']+0.1)*100):.1f}%"
        },
        "pricing": {
            "basic": "$67/month (launch) → $97/month (regular after 1 month)",
            "pro": "$147/month (launch) → $297/month (regular after 1 month)",
            "enterprise": "$497/month (launch) → $1500/month (regular after 1 month)"
        },
        "data_source": "Live signal extraction from your bot system"
    }

@app.post("/api/v1/subscribe")
async def create_subscription(subscription: Dict):
    """Create subscription with automatic pricing transition"""
    
    email = subscription.get('email')
    tier = subscription.get('tier', 'basic')
    currency = subscription.get('currency', 'usdterc20')
    
    if not email or tier not in ['basic', 'pro', 'enterprise']:
        raise HTTPException(status_code=400, detail="Valid email and tier required")
    
    return await payments.create_subscription_payment(email, tier, currency)

@app.post("/webhooks/nowpayments")
async def payment_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handle payment confirmations and activate subscriptions"""
    
    payload = await request.json()
    
    if payload.get('payment_status') == 'confirmed':
        background_tasks.add_task(
            payments.handle_payment_confirmation,
            payload['payment_id']
        )
    
    return {"status": "received"}

@app.get("/api/v1/signals/tier1")
async def get_tier1_signals(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get all arbitrage opportunities from your signal extractor"""
    
    customer = await customer_db.verify_api_access(credentials.credentials)
    
    if not customer['permissions']['tier1_access']:
        raise HTTPException(status_code=403, detail="Tier 1 access required")
    
    tier1_data = signal_manager.get_tier1_signals()
    
    return {
        "signals": tier1_data.get('signals', []),
        "total_opportunities": len(tier1_data.get('signals', [])),
        "customer_tier": customer['tier'],
        "pricing_info": {
            "current_price": customer['current_price'],
            "is_grandfathered": customer['is_grandfathered']
        },
        "last_updated": signal_manager.last_update.isoformat() if signal_manager.last_update else None,
        "data_source": "Live bot detection - all exchanges"
    }

@app.get("/api/v1/signals/tier2")
async def get_tier2_signals(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get validated arbitrage opportunities - Pro+ only"""
    
    customer = await customer_db.verify_api_access(credentials.credentials)
    
    if not customer['permissions']['tier2_access']:
        raise HTTPException(status_code=403, detail="Tier 2 access requires Pro or Enterprise subscription")
    
    tier2_data = signal_manager.get_tier2_signals()
    
    return {
        "validated_signals": tier2_data.get('signals_passed', []),
        "filtered_signals": tier2_data.get('signals_failed', [])[:10],  # Show some filtered for transparency
        "validation_summary": {
            "total_passed": len(tier2_data.get('signals_passed', [])),
            "total_failed": len(tier2_data.get('signals_failed', [])),
            "validation_accuracy": tier2_data.get('validation_summary', {})
        },
        "customer_tier": customer['tier'],
        "pricing_info": {
            "current_price": customer['current_price'],
            "is_grandfathered": customer['is_grandfathered']
        },
        "last_updated": signal_manager.last_update.isoformat() if signal_manager.last_update else None,
        "data_source": "Live bot validation engine"
    }

@app.get("/api/v1/customer/dashboard/{customer_id}")
async def get_customer_dashboard(customer_id: str):
    """Customer dashboard with pricing transition info"""
    
    with customer_db._get_db() as conn:
        customer = conn.execute('''
            SELECT * FROM customers WHERE id = ?
        ''', (customer_id,)).fetchone()
        
        if not customer:
            raise HTTPException(status_code=404, detail="Customer not found")
        
        # Get usage stats
        usage = conn.execute('''
            SELECT date, SUM(requests_count) as daily_requests
            FROM api_usage 
            WHERE customer_id = ? AND date >= date('now', '-7 days')
            GROUP BY date ORDER BY date DESC
        ''', (customer_id,)).fetchall()
        
        tier = customer['tier']
        regular_price = customer_db.pricing[tier]['regular']
        current_price = customer['current_price']
        
        return {
            "customer": dict(customer),
            "subscription": {
                "tier": tier,
                "status": customer['subscription_status'],
                "current_price": current_price,
                "regular_price": regular_price,
                "monthly_savings": regular_price - current_price,
                "is_grandfathered": bool(customer['is_grandfathered']),
                "launch_pricing_ends": customer['launch_pricing_ends_at'],
                "next_billing": customer['next_billing_date']
            },
            "api": {
                "key": customer['api_key'],
                "endpoints": {
                    "tier1": "GET /api/v1/signals/tier1",
                    "tier2": "GET /api/v1/signals/tier2" if customer['tier'] in ['pro', 'enterprise'] else "Upgrade required"
                },
                "permissions": customer_db._get_tier_permissions(customer['tier'])
            },
            "usage": [dict(u) for u in usage],
            "live_data": signal_manager.get_system_status()
        }

@app.post("/api/v1/admin/check-pricing-transitions")
async def check_pricing_transitions():
    """Admin endpoint to check and process pricing transitions"""
    await customer_db.check_pricing_transitions()
    return {"status": "pricing transitions checked"}

# Run the API server
if __name__ == "__main__":
    # Run initial signal extraction
    signal_manager.run_signal_extraction()
    
    # Start server
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)