import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Removed: import traceback - app.logger.exception handles it
import logging  # Import logging
from datetime import datetime, date
from typing import Optional
from decimal import Decimal, InvalidOperation

# Import the DB instance and model from models.py
# Import the DB instance AND ALL MODELS from models.py
from models import (
    db,
    DemandRecord,
    PndMdaRecord,
    PmlMdaRecord,
    PmlMtrRecord,
    PndMtrRecord,
    CapacidadTransferenciaRecord
)

# --- App Configuration -------------------------------------------------------
from urllib.parse import quote_plus  # NEW – to URL‑encode the ODBC connection string
    
load_dotenv()

app = Flask(__name__)
CORS(app)

if "DATABASE_URL" in os.environ:
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
else:
    DRIVER = os.getenv("DB_DRIVER")  # "ODBC Driver 1X for SQL Server"
    USER   = os.getenv("DB_USERNAME")  # "
    PASS   = os.getenv("DB_PASSWORD")
    SERVER = os.getenv("DB_SERVER")   # "hostname,1433" if a custom port
    DBNAME = os.getenv("DB_DATABASE")

    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DBNAME};"
        f"UID={USER};PWD={PASS};"
        "Encrypt=yes;"  # keep traffic encrypted
        "TrustServerCertificate=yes;"  # <<< added (skip cert validation) *
    )
    if USER is None or PASS is None:
        app.logger.error(
            "Database credentials (DB_USER, DB_PASSWORD) are not set in environment variables."
        )
        print(USER, PASS)
        raise ValueError(
            "Database credentials (DB_USER, DB_PASSWORD) are not set in environment variables."
        )

    params = quote_plus(odbc_str)
    app.config["SQLALCHEMY_DATABASE_URI"] = f"mssql+pyodbc:///?odbc_connect={params}"

log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    filename="mercado.log",  # Log to a file
    level=log_level,
)  # Basic config affects root logger

db.init_app(app)

# --- Database Creation ---
with app.app_context():
    # Use app.logger now
    app.logger.info("Creating database tables if they don't exist...")
    try:
        if "sqlite" in app.config["SQLALCHEMY_DATABASE_URI"] and not os.path.exists(
            app.instance_path
        ):
            os.makedirs(app.instance_path, exist_ok=True)
            app.logger.info(f"Using SQLite database in: {app.instance_path}")
        db.create_all()
        app.logger.info("Database tables checked/created successfully.")
    except Exception as e:
        # Use logger.exception to include traceback automatically
        app.logger.exception(
            f"CRITICAL ERROR: Failed to create/check database tables: {e}"
        )


# --- API Endpoints ---
@app.route("/")
def index():
    """Basic index route showing API info"""
    return jsonify(
        {
            "message": "CENACE Data Storage API",
            "status": "running",
            "endpoints": {"submit": "/submit-data (POST)"},
            "timestamp": datetime.now().isoformat(),
        }
    )




# --- Run App ---
if __name__ == "__main__":
    # When running directly, make sure logging is set up before app.run
    # (basicConfig call above handles this for simple cases)
    app.run(debug=True, host="0.0.0.0", port=5001)
