from flask import Blueprint, request, jsonify, current_app
from datetime import datetime, date
from decimal import InvalidOperation

from ... import db
from ...models.pml_pnd_records import (
    PndMdaRecord,
    PmlMdaRecord,
    PmlMtrRecord,
    PndMtrRecord,
)

DATA_TYPE_MODELS = {
    "pnd_mda": PndMdaRecord,
    "pml_mda": PmlMdaRecord,
    "pml_mtr": PmlMtrRecord,
    "pnd_mtr": PndMtrRecord,
}

generic_mda_mtr_bp = Blueprint("generic_mda_mtr", __name__)


# --- NEW: Endpoint to Check if Data Exists for a Date ---
@generic_mda_mtr_bp.route("/<string:data_type>/<string:fecha>", methods=["GET"])
def check_data_existence(data_type, fecha):
    """
    Checks if any data exists for the given data_type and fecha (YYYY-MM-DD).
    Returns {"exists": true} or {"exists": false}.
    """
    model_key = data_type.lower()
    ModelClass = DATA_TYPE_MODELS.get(model_key)

    # 1. Validate data_type
    if ModelClass is None:
        current_app.logger.warning(
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
        current_app.logger.warning(
            f"Check request received for invalid fecha format '{fecha}'."
        )
        return jsonify(
            {
                "status": "error",
                "message": f"Invalid fecha format: '{fecha}'. Expected YYYY-MM-DD.",
            }
        ), 400

    try:
        result = (
            db.session.query(ModelClass.Sistema)
            .filter_by(Fecha=parsed_date)
            .limit(1)
            .first()
        )

        # If 'first()' returned anything (a tuple/object), it exists. If it returned None, it doesn't.
        exists = result is not None

        current_app.logger.info(
            f"Check result for {model_key} on {fecha}: {'Exists' if exists else 'Does not exist'}"
        )
        return jsonify({"exists": bool(exists)}), 200

    except Exception as e:
        current_app.logger.exception(
            f"Database error during existence check for {model_key} on {fecha}: {e}"
        )
        return jsonify(
            {"status": "error", "message": "Database query failed during check."}
        ), 500


# --- MODIFIED: Endpoint to Insert Batches (Assumes Date is Clear) ---
# Removed the complex upsert logic, focusing only on insertion.
@generic_mda_mtr_bp.route("/<string:data_type>", methods=["POST"])
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
        current_app.logger.warning(
            f"Insert request received for invalid data_type '{data_type}'."
        )
        valid_types = list(DATA_TYPE_MODELS.keys())
        return jsonify(
            {
                "status": "error",
                "message": f"Invalid data type specified: '{data_type}'. Valid types are: {valid_types}",
            }
        ), 404

    current_app.logger.info(
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
        current_app.logger.info(f"Received empty batch list for '{model_key}'.")
        # Return success, but indicate nothing was inserted from this batch
        summary["inserted"] = 0
        return jsonify({"status": "success", "summary": summary, "errors": []}), 200

    current_app.logger.info(
        f"Attempting to insert batch of {summary['total_records_received']} '{model_key}' records."
    )

    # 2. Prepare and Add Objects to Session (No Lookups)
    objects_to_insert = []
    for index, record_dict in enumerate(records_list):
        if not isinstance(record_dict, dict):
            current_app.logger.warning(
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
            current_app.logger.warning(
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
            current_app.logger.info(
                f"Commit successful for '{model_key}' batch. Inserted: {summary['inserted']}"
            )

        except Exception as db_commit_err:
            db.session.rollback()
            current_app.logger.exception(
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
        current_app.logger.warning(
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
    current_app.logger.info(
        f"'{model_key}' insert batch request finished in {duration:.2f} seconds. Status: {final_status}. Summary: {summary}"
    )
    response_body = {
        "status": final_status,
        "summary": summary,
        "errors": record_errors,
    }
    return jsonify(response_body), http_code