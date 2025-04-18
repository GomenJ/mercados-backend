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
)

# --- App Configuration -------------------------------------------------------
from urllib.parse import quote_plus  # NEW – to URL‑encode the ODBC connection string

app = Flask(__name__)
CORS(app)

DATA_TYPE_MODELS = {
    "pnd_mda": PndMdaRecord,
    "pml_mda": PmlMdaRecord,
    "pml_mtr": PmlMtrRecord,
    "pnd_mtr": PndMtrRecord,
}

if "DATABASE_URL" in os.environ:
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ["DATABASE_URL"]
else:
    DRIVER = "ODBC Driver 18 for SQL Server"  # or 18, 11, etc.

    USER = os.getenv("DB_USER", "usr_EnergyTrack")  # "
    PASS = os.getenv("DB_PASSWORD", "Z4YH5MfDhNFIVoOcWxeV")
    SERVER = os.getenv(
        "DB_SERVER", "192.168.200.210"
    )  # "hostname,1433" if a custom port
    DBNAME = os.getenv("DB_NAME", "InfoMercado")

    odbc_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DBNAME};"
        f"UID={USER};PWD={PASS};"
        "Encrypt=yes;"  # keep traffic encrypted
        "TrustServerCertificate=yes;"  # <<< added (skip cert validation) *
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


@app.route("/api/v1/mercado/demanda", methods=["POST"])
def submit_data_single():
    """
    Receives a SINGLE processed data record via JSON POST request
    and performs database INSERT/UPDATE/CONFLICT logic.
    """
    request_start_time = datetime.now()
    # Use app.logger for logging within the request context
    app.logger.info(
        f"Request received on /submit-data at {request_start_time.isoformat()}"
    )
    outcome = {"status": "unknown", "action": "none", "message": ""}
    http_code = 500  # Default to server error

    # 1. Validate Request
    if not request.is_json:
        app.logger.error("Request content type is not application/json")
        return jsonify(
            {
                "status": "error",
                "message": "Request header 'Content-Type' must be 'application/json'",
            }
        ), 415

    try:
        record_dict = request.get_json()
        if not isinstance(record_dict, dict):
            app.logger.error(
                f"Received data is not a dictionary (Type: {type(record_dict)})."
            )
            return jsonify(
                {
                    "status": "error",
                    "message": "Invalid payload format: body must be a single JSON object.",
                }
            ), 400
    except Exception as e:
        app.logger.error(f"Failed to parse incoming JSON: {e}")
        return jsonify(
            {"status": "error", "message": "Failed to parse request body as JSON."}
        ), 400

    app.logger.info(
        f"Received record content: {record_dict}"
    )  # Log received data (be mindful of sensitive data in production)

    # 2. Extract and Validate Key components
    fecha_op_str = record_dict.get("FechaOperacion")
    hora = record_dict.get("HoraOperacion")
    gerencia = record_dict.get("Gerencia")

    if fecha_op_str is None or hora is None or gerencia is None:
        app.logger.warning(f"Record missing key components: {record_dict}. Rejecting.")
        return jsonify(
            {
                "status": "error",
                "message": "Record missing required key fields (FechaOperacion, HoraOperacion, Gerencia).",
            }
        ), 400

    try:
        fecha_op = date.fromisoformat(fecha_op_str)
        hora = int(hora)
        if not (0 <= hora <= 23):
            raise ValueError("Hour must be between 0 and 23")
    except (ValueError, TypeError) as conv_err:
        app.logger.warning(
            f"Invalid key format in record: {record_dict}. Error: {conv_err}. Rejecting."
        )
        return jsonify(
            {"status": "error", "message": f"Invalid key format: {conv_err}"}
        ), 400

    # 3. Process Record and Interact with Database
    pk = None
    try:
        pk = (fecha_op, hora, gerencia)
        # existing_record: Optional[DemandRecord] = db.session.get(DemandRecord, pk)
        existing_record: Optional[DemandRecord] = DemandRecord.query.filter_by(
            FechaOperacion=fecha_op, HoraOperacion=hora, Gerencia=gerencia
        ).first()

        if existing_record is None:
            # --- INSERT ---
            app.logger.info(f"Record {pk} not found. Attempting INSERT.")

            new_record = DemandRecord(
                FechaOperacion=fecha_op,
                HoraOperacion=hora,
                Gerencia=gerencia,
                Demanda=record_dict.get("Demanda"),
                Generacion=record_dict.get("Generacion"),
                Pronostico=record_dict.get("Pronostico"),
                Enlace=record_dict.get("Enlace"),
                Sistema=record_dict.get("Sistema", "UNK"),
            )
            db.session.add(new_record)
            db.session.commit()
            app.logger.info(f"INSERT successful for {pk}")
            outcome = {
                "status": "success",
                "action": "inserted",
                "message": f"Record {pk} inserted.",
            }
            http_code = 201
        else:
            # --- CHECK FOR UPDATE or CONFLICT ---
            if existing_record.data_is_different(record_dict):
                # --- UPDATE ---
                app.logger.info(
                    f"Record {pk} found. Data is different. Attempting UPDATE."
                )
                existing_record.Demanda = record_dict.get("Demanda")
                existing_record.Generacion = record_dict.get("Generacion")
                existing_record.Pronostico = record_dict.get("Pronostico")
                existing_record.Enlace = record_dict.get("Enlace")
                db.session.commit()
                app.logger.info(f"UPDATE successful for record id {existing_record.id}")
                outcome = {
                    "status": "success",
                    "action": "updated",
                    "message": f"Record id {existing_record.id} ({pk}) updated.",
                }
                http_code = 200
            else:
                # --- CONFLICT ---
                app.logger.info(
                    f"CONFLICT (Identical) for record id {existing_record.id} ({pk}). No changes made."
                )
                outcome = {
                    "status": "conflict",
                    "action": "none",
                    "message": f"Record {pk} already exists with identical data.",
                }
                http_code = 409

    except Exception as db_err:
        # Use logger.exception to log the error *with* traceback
        app.logger.exception(f"Database operation failed for {pk}: {db_err}")
        db.session.rollback()
        outcome = {
            "status": "error",
            "action": "error",
            "message": f"Database error processing record {pk}.",
        }
        http_code = 500

    # 4. Return Final Response
    request_end_time = datetime.now()
    duration = (request_end_time - request_start_time).total_seconds()
    app.logger.info(
        f"Request finished in {duration:.2f} seconds. Outcome: {outcome.get('status', 'error')}"
    )

    return jsonify(outcome), http_code


# --- NEW: Endpoint to Check if Data Exists for a Date ---
@app.route("/api/v1/mercado/check/<string:data_type>/<string:fecha>", methods=["GET"])
def check_data_existence(data_type, fecha):
    """
    Checks if any data exists for the given data_type and fecha (YYYY-MM-DD).
    Returns {"exists": true} or {"exists": false}.
    """
    model_key = data_type.lower()
    ModelClass = DATA_TYPE_MODELS.get(model_key)

    # 1. Validate data_type
    if ModelClass is None:
        app.logger.warning(
            f"Check request received for invalid data_type '{data_type}'."
        )
        valid_types = list(DATA_TYPE_MODELS.keys())
        return jsonify(
            {
                "status": "error",
                "message": f"Invalid data type specified: '{data_type}'. Valid types are: {valid_types}",
            }
        ), 404

    # 2. Validate and parse fecha
    try:
        parsed_date = date.fromisoformat(fecha)
    except ValueError:
        app.logger.warning(
            f"Check request received for invalid fecha format '{fecha}'."
        )
        return jsonify(
            {
                "status": "error",
                "message": f"Invalid fecha format: '{fecha}'. Expected YYYY-MM-DD.",
            }
        ), 400

    # 3. Perform Database Query
    try:
        # Use an efficient query like EXISTS or query().limit(1).count() > 0
        # query().exists().scalar() is often efficient
        # exists = db.session.query(
        #     ModelClass.query.filter_by(Fecha=parsed_date).exists()
        # ).scalar()
        result = (
            db.session.query(ModelClass.Sistema)
            .filter_by(Fecha=parsed_date)
            .limit(1)
            .first()
        )

        # If 'first()' returned anything (a tuple/object), it exists. If it returned None, it doesn't.
        exists = result is not None

        app.logger.info(
            f"Check result for {model_key} on {fecha}: {'Exists' if exists else 'Does not exist'}"
        )
        return jsonify({"exists": bool(exists)}), 200

    except Exception as e:
        app.logger.exception(
            f"Database error during existence check for {model_key} on {fecha}: {e}"
        )
        return jsonify(
            {"status": "error", "message": "Database query failed during check."}
        ), 500


# --- MODIFIED: Endpoint to Insert Batches (Assumes Date is Clear) ---
# Removed the complex upsert logic, focusing only on insertion.
@app.route("/api/v1/mercado/mda_mtr/<string:data_type>", methods=["POST"])
def submit_generic_batch_insert_only(data_type):
    """
    Receives a BATCH of records via JSON POST (list of dicts).
    ASSUMES the client has already verified that no data exists for this date.
    Performs fast batch inserts. Does NOT check for duplicates/updates.
    """
    request_start_time = datetime.now()
    model_key = data_type.lower()
    ModelClass = DATA_TYPE_MODELS.get(model_key)

    if ModelClass is None:
        app.logger.warning(
            f"Insert request received for invalid data_type '{data_type}'."
        )
        valid_types = list(DATA_TYPE_MODELS.keys())
        return jsonify(
            {
                "status": "error",
                "message": f"Invalid data type specified: '{data_type}'. Valid types are: {valid_types}",
            }
        ), 404

    app.logger.info(
        f"Insert batch request received for data_type '{model_key}' at {request_start_time.isoformat()}"
    )

    summary = {
        "total_records_received": 0,
        "inserted": 0,
        "failed_validation": 0,
        "database_errors": 0,
    }
    record_errors = []

    # 1. Validate Request is JSON and is a List
    if not request.is_json:
        # ... (handle as before) ...
        return jsonify(
            {"status": "error", "message": "'Content-Type' must be 'application/json'"}
        ), 415
    try:
        records_list = request.get_json()
        if not isinstance(records_list, list):
            # ... (handle as before) ...
            return jsonify(
                {"status": "error", "message": "Payload must be a JSON list"}
            ), 400
    except Exception as e:
        # ... (handle as before) ...
        return jsonify({"status": "error", "message": "Failed to parse JSON list"}), 400

    summary["total_records_received"] = len(records_list)
    if not records_list:
        app.logger.info(f"Received empty batch list for '{model_key}'.")
        # Return success, but indicate nothing was inserted from this batch
        summary["inserted"] = 0
        return jsonify({"status": "success", "summary": summary, "errors": []}), 200

    app.logger.info(
        f"Attempting to insert batch of {summary['total_records_received']} '{model_key}' records."
    )

    # 2. Prepare and Add Objects to Session (No Lookups)
    objects_to_insert = []
    for index, record_dict in enumerate(records_list):
        if not isinstance(record_dict, dict):
            app.logger.warning(
                f"Item at index {index} for '{model_key}' not a dictionary. Skipping."
            )
            summary["failed_validation"] += 1
            record_errors.append(
                {"index": index, "error": "Item not an object.", "data": record_dict}
            )
            continue

        # Perform minimal validation before creating object
        try:
            # Basic check for required keys (adjust if needed)
            required_keys = {"Sistema", "Fecha", "Hora", "Clave"}
            if not required_keys.issubset(record_dict.keys()):
                raise ValueError(
                    "Missing required key fields (Sistema, Fecha, Hora, Clave)."
                )

            # Optional: Deeper validation (like date/hour format) if desired,
            # but keep it fast as the goal here is speed.
            # Convert types cautiously before passing to ModelClass
            validated_data = {
                "Sistema": str(record_dict["Sistema"]),
                "Fecha": date.fromisoformat(str(record_dict["Fecha"])),
                "Hora": int(record_dict["Hora"]),
                "Clave": str(record_dict["Clave"]),
                "PML": record_dict.get(
                    "PML"
                ),  # Pass raw value, let model handle Decimal
                "Energia": record_dict.get("Energia"),
                "Congestion": record_dict.get("Congestion"),
                "Perdidas": record_dict.get("Perdidas"),
            }
            # Check constraints again after conversion
            if not (0 <= validated_data["Hora"] <= 24):
                raise ValueError("Invalid Hora range")
            if len(validated_data["Sistema"]) > 3:
                raise ValueError("Sistema too long")
            if len(validated_data["Clave"]) > 20:
                raise ValueError("Clave too long")

            # Create ORM object directly
            new_record = ModelClass(**validated_data)
            objects_to_insert.append(new_record)

        except (ValueError, TypeError, KeyError, InvalidOperation) as validation_err:
            app.logger.warning(
                f"Validation failed for '{model_key}' record at index {index}: {validation_err}. Data: {record_dict}"
            )
            summary["failed_validation"] += 1
            record_errors.append(
                {"index": index, "error": str(validation_err), "data": record_dict}
            )
            # Continue to the next record

    # 3. Perform Database Commit
    final_status = "success"
    http_code = 200  # Or 201 if you prefer for successful inserts

    if objects_to_insert:  # Only proceed if there are valid objects to insert
        try:
            db.session.add_all(
                objects_to_insert
            )  # Use add_all for potential efficiency
            db.session.commit()
            summary["inserted"] = len(
                objects_to_insert
            )  # Count successfully prepared objects
            app.logger.info(
                f"Commit successful for '{model_key}' batch. Inserted: {summary['inserted']}"
            )

        except Exception as db_commit_err:
            db.session.rollback()
            app.logger.exception(
                f"Database error during '{model_key}' batch commit: {db_commit_err}"
            )
            summary["database_errors"] = len(
                objects_to_insert
            )  # All attempted inserts failed
            summary["inserted"] = 0
            record_errors.append(
                {
                    "error": f"Database commit failed: {db_commit_err}. Batch rolled back.",
                    "data": "N/A",
                }
            )
            final_status = "error"
            http_code = 500
    else:
        app.logger.warning(
            f"No valid records to insert for '{model_key}' in this batch after validation."
        )
        # If all failed validation, report partial success; if list was empty, status is already success
        if summary["failed_validation"] > 0:
            final_status = "partial_success"
            http_code = 207

    # Adjust final status code if needed
    if summary["failed_validation"] > 0 and final_status != "error":
        final_status = "partial_success"
        http_code = 207  # Multi-Status

    # 4. Return Final Response
    request_end_time = datetime.now()
    duration = (request_end_time - request_start_time).total_seconds()
    app.logger.info(
        f"'{model_key}' insert batch request finished in {duration:.2f} seconds. Status: {final_status}. Summary: {summary}"
    )
    response_body = {
        "status": final_status,
        "summary": summary,
        "errors": record_errors,
    }
    return jsonify(response_body), http_code


# --- Run App ---
if __name__ == "__main__":
    # When running directly, make sure logging is set up before app.run
    # (basicConfig call above handles this for simple cases)
    load_dotenv()
    app.run(debug=True, host="0.0.0.0", port=5001)
