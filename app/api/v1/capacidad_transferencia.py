# app/api/v1/cotizacion_routes.py
from flask import Blueprint, request, jsonify, current_app
from datetime import datetime, date
from ...models.capacidad_transferencia_record import CapacidadTransferenciaRecord
from ... import db


capacidad_transferencia_bp = Blueprint('capacidad_transferencia', __name__)

# --- NEW: Endpoint for CapacidadTransferencia Batch Inserts ---
@capacidad_transferencia_bp.route("", methods=["POST"])
def submit_capacidad_transferencia_batch():
    """
    Receives a BATCH of CapacidadTransferencia records via JSON POST (list of dicts).
    Performs fast batch inserts. Assumes the client handles data clearing logic.
    Does NOT check for duplicates/updates before inserting.
    """
    request_start_time = datetime.now()
    ModelClass = CapacidadTransferenciaRecord # Use the specific model class
    model_name = "CapacidadTransferencia"    # For logging and error messages

    current_app.logger.info(
        f"Insert batch request received for '{model_name}' at {request_start_time.isoformat()}"
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
        return jsonify({"status": "error", "message": "'Content-Type' must be 'application/json'"}), 415
    try:
        records_list = request.get_json()
        if not isinstance(records_list, list):
            return jsonify({"status": "error", "message": "Payload must be a JSON list"}), 400
    except Exception as e:
         current_app.logger.warning(f"Failed to parse JSON for {model_name}: {e}")
         return jsonify({"status": "error", "message": "Failed to parse JSON list"}), 400

    summary["total_records_received"] = len(records_list)
    if not records_list:
        current_app.logger.info(f"Received empty batch list for '{model_name}'.")
        summary["inserted"] = 0
        return jsonify({"status": "success", "summary": summary, "errors": []}), 200

    current_app.logger.info(
        f"Attempting to insert batch of {summary['total_records_received']} '{model_name}' records."
    )

    # 2. Prepare and Add Objects to Session (No Lookups)
    objects_to_insert = []
    for index, record_dict in enumerate(records_list):
        if not isinstance(record_dict, dict):
            current_app.logger.warning(f"Item at index {index} for '{model_name}' not a dictionary. Skipping.")
            summary["failed_validation"] += 1
            record_errors.append({"index": index, "error": "Item not an object.", "data": record_dict})
            continue

        # Perform validation before creating object
        try:
            # Define required keys for CapacidadTransferencia
            # Adjust if some fields are optional in your source data
            required_keys = {
                "Sistema", "FechaOperacion", "Enlace", "Horario",
                "CapTransDisImpComMwh", "CapResImpEneInadMwh", "CapResImpConfMWh", "CapAbsTransDisImpMWh",
                "CapTransDisExpComMwh", "CapResExpEneInaMwh", "CapResExpConfMwh", "CapAbsTransDisExpMwh"
            }
            if not required_keys.issubset(record_dict.keys()):
                missing = required_keys - record_dict.keys()
                raise ValueError(f"Missing required key fields: {', '.join(missing)}")

            # Convert types carefully and create validated data dictionary
            validated_data = {
                "Sistema": str(record_dict["Sistema"]),
                "FechaOperacion": date.fromisoformat(str(record_dict["FechaOperacion"])), # Ensure input is YYYY-MM-DD string
                "Enlace": str(record_dict["Enlace"]),
                "Horario": int(record_dict["Horario"]),
                "CapTransDisImpComMwh": int(record_dict["CapTransDisImpComMwh"]),
                "CapResImpEneInadMwh": int(record_dict["CapResImpEneInadMwh"]),
                "CapResImpConfMWh": int(record_dict["CapResImpConfMWh"]),
                "CapAbsTransDisImpMWh": int(record_dict["CapAbsTransDisImpMWh"]),
                "CapTransDisExpComMwh": int(record_dict["CapTransDisExpComMwh"]),
                "CapResExpEneInaMwh": int(record_dict["CapResExpEneInaMwh"]), # Ensure key matches model/DB ('Ina' vs 'Inad')
                "CapResExpConfMwh": int(record_dict["CapResExpConfMwh"]),
                "CapAbsTransDisExpMwh": int(record_dict["CapAbsTransDisExpMwh"]),
            }

            # Check constraints again after conversion
            # Adjust range if Horario is 0-23 instead of 1-24
            if not (1 <= validated_data["Horario"] <= 24):
                raise ValueError("Invalid Horario range (expected 1-24)")
            if len(validated_data["Sistema"]) > 3:
                raise ValueError("Sistema value too long (max 3 characters)")
            if len(validated_data["Enlace"]) > 32:
                raise ValueError("Enlace value too long (max 32 characters)")
            # Add any other necessary checks (e.g., non-negative capacities)
            # Example:
            # if validated_data["CapTransDisImpComMwh"] < 0:
            #     raise ValueError("CapTransDisImpComMwh cannot be negative")

            # Create ORM object directly using validated data
            new_record = ModelClass(**validated_data)
            objects_to_insert.append(new_record)

        # Catch specific errors related to validation/type conversion
        except (ValueError, TypeError, KeyError) as validation_err:
            current_app.logger.warning(
                f"Validation failed for '{model_name}' record at index {index}: {validation_err}. Data: {record_dict}"
            )
            summary["failed_validation"] += 1
            record_errors.append({"index": index, "error": str(validation_err), "data": record_dict})
            # Continue processing the rest of the batch

    # 3. Perform Database Commit
    final_status = "success"
    http_code = 200 # Use 200 for OK or 201 for Created

    if objects_to_insert: # Only attempt commit if there are valid objects
        try:
            # Use add_all for potential efficiency with many objects
            db.session.add_all(objects_to_insert)
            db.session.commit()
            summary["inserted"] = len(objects_to_insert)
            current_app.logger.info(
                f"Commit successful for '{model_name}' batch. Inserted: {summary['inserted']}"
            )

        except Exception as db_commit_err:
            # Rollback the session in case of any database error during commit
            db.session.rollback()
            current_app.logger.exception( # Log the full traceback
                f"Database error during '{model_name}' batch commit: {db_commit_err}"
            )
            summary["database_errors"] = len(objects_to_insert) # All attempted inserts in this batch failed
            summary["inserted"] = 0
            # Add a general error for the batch failure
            record_errors.append({
                "index": "N/A", # Error applies to the whole batch commit
                "error": f"Database commit failed: {type(db_commit_err).__name__}. Batch rolled back. Check logs for details.",
                "data": "N/A",
            })
            final_status = "error"
            http_code = 500 # Internal Server Error
    else:
        # Case where the list was not empty initially, but all records failed validation
        current_app.logger.warning(
            f"No valid records to insert for '{model_name}' in this batch after validation."
        )
        if summary["failed_validation"] > 0:
            final_status = "partial_success" # Or "error" depending on desired outcome
            http_code = 207 # Multi-Status, indicates some failed

    # Adjust final status/code if some validations failed but DB operation (or lack thereof) succeeded
    if summary["failed_validation"] > 0 and final_status == "success":
        final_status = "partial_success"
        http_code = 207 # Multi-Status

    # 4. Return Final Response
    request_end_time = datetime.now()
    duration = (request_end_time - request_start_time).total_seconds()
    current_app.logger.info(
        f"'{model_name}' insert batch request finished in {duration:.2f} seconds. Status: {final_status}. Summary: {summary}"
    )
    response_body = {
        "status": final_status,
        "summary": summary,
        "errors": record_errors, # List of validation/commit errors
    }
    return jsonify(response_body), http_code
