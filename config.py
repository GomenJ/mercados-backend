import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'default-secret-key-for-dev')
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False # Disable modification tracking
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL')
    if not SQLALCHEMY_DATABASE_URI:
        # Construct from individual parts if DATABASE_URL is not set
        SERVER = os.getenv("SERVER")
        DATABASE = os.getenv("DATABASE")
        DB_USERNAME = os.getenv("DB_USERNAME") # Using DB_USER as recommended to avoid system USERNAME conflict
        PASSWORD = os.getenv("DB_PASSWORD")
        if all([SERVER, DATABASE, DB_USERNAME, PASSWORD]):
            SQLALCHEMY_DATABASE_URI = (
                f"mssql+pyodbc://{DB_USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
                f"?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
            )
        else:
            # Handle error or set a default if necessary, though app creation might fail
            print("Warning: Database connection details not fully configured via individual env vars.")
   # --- SQLAlchemy Connection Pooling Settings ---
    SQLALCHEMY_POOL_SIZE = int(os.getenv("SQLALCHEMY_POOL_SIZE", 10)) # Default to 10 connections
    SQLALCHEMY_POOL_TIMEOUT = int(os.getenv("SQLALCHEMY_POOL_TIMEOUT", 20)) # Default timeout of 20 seconds
    SQLALCHEMY_POOL_RECYCLE = int(os.getenv("SQLALCHEMY_POOL_RECYCLE", 3600)) # Recycle connections every hour
    SQLALCHEMY_MAX_OVERFLOW = int(os.getenv("SQLALCHEMY_MAX_OVERFLOW", 5)) # Allow up to 5 connections beyond pool size


class DevelopmentConfig(Config):
    DEBUG = True
    LOGGING_LEVEL = 'DEBUG'
    # SQLALCHEMY_ECHO = True # Useful for seeing generated SQL

class ProductionConfig(Config):
    # Production specific settings
    # For example, if you need to enforce SSL for DB connection:
    # SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL_PROD')
    SQLALCHEMY_POOL_SIZE = int(os.getenv("SQLALCHEMY_POOL_SIZE_PROD", 20))
    SQLALCHEMY_MAX_OVERFLOW = int(os.getenv("SQLALCHEMY_MAX_OVERFLOW_PROD", 10))
    pass

# Dictionary to access config by name
app_config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
