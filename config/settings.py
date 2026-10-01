import os
from pathlib import Path
from dotenv import load_dotenv

# Base directory of the project
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file
load_dotenv(BASE_DIR / ".env")

# Global Shopify App Settings (Standalone OAuth 2.0)
SHOPIFY_CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID", "").strip()
SHOPIFY_CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET", "").strip()
SHOPIFY_REDIRECT_URI = os.getenv("SHOPIFY_REDIRECT_URI", "http://localhost:8000/auth/shopify/callback").strip()
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-07").strip()
SHOPIFY_SCOPES = os.getenv(
    "SHOPIFY_SCOPES",
    "read_products,read_orders,read_customers,read_inventory,read_locations"
).strip()

# Security & Encryption (Fernet AES-128-CBC + HMAC-SHA256)
STORE_TOKEN_ENCRYPTION_KEY = os.getenv("STORE_TOKEN_ENCRYPTION_KEY", "").strip()

# Business Context (Default fallback)
BUSINESS_ID = int(os.getenv("BUSINESS_ID", "1"))

# Database Settings (PostgreSQL 18)
DB_HOST = os.getenv("DB_HOST", "localhost").strip()
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ecommerce_intelligence").strip()
DB_USER = os.getenv("DB_USER", "postgres").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()

def get_db_url(db_name: str = None) -> str:
    """
    Constructs a SQLAlchemy connection URI for PostgreSQL.
    """
    target_db = db_name if db_name else DB_NAME
    if DB_PASSWORD:
        return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{target_db}"
    return f"postgresql+psycopg2://{DB_USER}@{DB_HOST}:{DB_PORT}/{target_db}"
