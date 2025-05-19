import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables from .env file
load_dotenv()

class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'default-secret-key-for-dev')
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False # Disable modification tracking
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

    # Database Configuration
    DB_DRIVER = os.getenv("DB_DRIVER")
    DB_USER = os.getenv("DB_USERNAME")
    DB_PASS = os.getenv("DB_PASSWORD")
    DB_SERVER = os.getenv("DB_SERVER")
    DB_NAME = os.getenv("DB_DATABASE")

    # Construct SQLAlchemy Database URI
    SQLALCHEMY_ENGINE_OPTIONS = {
    'fast_executemany': True,
    'use_setinputsizes': False  # This is often recommended alongside fast_executemany for performance
    }
    if "DATABASE_URL" in os.environ:
        SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]
    elif DB_DRIVER and DB_USER and DB_PASS and DB_SERVER and DB_NAME:
        odbc_str = (
            f"DRIVER={{{DB_DRIVER}}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};PWD={DB_PASS};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;" # Adjust based on your security needs
        )
        params = quote_plus(odbc_str)
        SQLALCHEMY_DATABASE_URI = f"mssql+pyodbc:///?odbc_connect={params}"
    else:
        # Fallback or raise error if essential DB vars are missing
        # Using SQLite in-memory as a fallback for demonstration
        print("WARNING: Database environment variables not fully set. Falling back to SQLite in-memory.")
        SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
        # Alternatively, raise an error:
        # raise ValueError("Database configuration variables missing in environment.")
    


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    #SQLALCHEMY_ECHO = True # Uncomment to see SQL queries


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    # Add production-specific settings here (e.g., stricter logging)


# Dictionary to access config classes by name
config_by_name = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}