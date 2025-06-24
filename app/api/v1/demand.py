from datetime import datetime, date
from flask import Blueprint, request, jsonify, current_app
from ...models.demand_record import DemandRecord 
from typing import Optional
from ... import db
from sqlalchemy.exc import SQLAlchemyError # Import for more specific DB errors
from sqlalchemy.exc import IntegrityError # For handling unique constraint violations
from ...services.demanta_tiempo_real_service import get_sin_demand_comparison, get_demanda_aggregates_for_comparison_dates

demanda_bp = Blueprint('demanda', __name__)

@demanda_bp.route("/current_day", methods=["GET"])
def get_current_day_demand():
    """
    Retrieves demand data for the current day.
    """
    try:
        today = date.today()
        current_app.logger.info(f"Attempting to retrieve demand data for date: {today}")

        demand_records = DemandRecord.query.filter_by(FechaOperacion=today).all()

        # Convert records to a list of dictionaries
        demand_data = [record.to_dict() for record in demand_records]

        current_app.logger.info(f"Retrieved {len(demand_data)} records for {today}")
        return jsonify(demand_data), 200

    except Exception as e:
        current_app.logger.exception(f"Error retrieving demand data for current day: {e}")
        return jsonify({"status": "error", "message": "Failed to retrieve demand data."}), 500

@demanda_bp.route("", methods=["POST"])
def submit_data_single():
    """
    Receives a SINGLE processed data record via JSON POST request
    and performs database INSERT/UPDATE/CONFLICT logic.
    """
    request_start_time = datetime.now()
    # Use current_app.logger for logging within the request context
    current_app.logger.info(
        f"Request received on /submit-data at {request_start_time.isoformat()}"
    )
    outcome = {"status": "unknown", "action": "none", "message": ""}
    http_code = 500  # Default to server error

    # 1. Validate Request
    if not request.is_json:
        current_app.logger.error("Request content type is not application/json")
        return jsonify(
            {
                "status": "error",
                "message": "Request header 'Content-Type' must be 'application/json'",
            }
        ), 415

    try:
        record_dict = request.get_json()
        if not isinstance(record_dict, dict):
            current_app.logger.error(
                f"Received data is not a dictionary (Type: {type(record_dict)})."
            )
            return jsonify(
                {
                    "status": "error",
                    "message": "Invalid payload format: body must be a single JSON object.",
                }
            ), 400
    except Exception as e:
        current_app.logger.error(f"Failed to parse incoming JSON: {e}")
        return jsonify(
            {"status": "error", "message": "Failed to parse request body as JSON."}
        ), 400

    current_app.logger.info(
        f"Received record content: {record_dict}"
    )  # Log received data (be mindful of sensitive data in production)

    # 2. Extract and Validate Key components
    fecha_op_str = record_dict.get("FechaOperacion")
    hora = record_dict.get("HoraOperacion")
    gerencia = record_dict.get("Gerencia")

    if fecha_op_str is None or hora is None or gerencia is None:
        current_app.logger.warning(f"Record missing key components: {record_dict}. Rejecting.")
        return jsonify(
            {
                "status": "error",
                "message": "Record missing required key fields (FechaOperacion, HoraOperacion, Gerencia).",
            }
        ), 400

    try:
        fecha_op = date.fromisoformat(fecha_op_str)
        hora = int(hora)
        if not (0 <= hora <= 24):
            raise ValueError("Hour must be between 0 and 24")
    except (ValueError, TypeError) as conv_err:
        current_app.logger.warning(
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
            current_app.logger.info(f"Record {pk} not found. Attempting INSERT.")

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
            current_app.logger.info(f"INSERT successful for {pk}")
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
                current_app.logger.info(
                    f"Record {pk} found. Data is different. Attempting UPDATE."
                )
                existing_record.Demanda = record_dict.get("Demanda")
                existing_record.Generacion = record_dict.get("Generacion")
                existing_record.Pronostico = record_dict.get("Pronostico")
                existing_record.Enlace = record_dict.get("Enlace")
                db.session.commit()
                current_app.logger.info(f"UPDATE successful for record id {existing_record.id}")
                outcome = {
                    "status": "success",
                    "action": "updated",
                    "message": f"Record id {existing_record.id} ({pk}) updated.",
                }
                http_code = 200
            else:
                # --- CONFLICT ---
                current_app.logger.info(
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
        current_app.logger.exception(f"Database operation failed for {pk}: {db_err}")
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
    current_app.logger.info(
        f"Request finished in {duration:.2f} seconds. Outcome: {outcome.get('status', 'error')}"
    )

    return jsonify(outcome), http_code

@demanda_bp.route("/bulk", methods=["POST"])
def submit_data_bulk():
    """
    Receives an ARRAY of processed data records via JSON POST request
    and attempts to INSERT all of them. If any record violates a unique
    constraint (e.g., duplicate), the entire batch is rolled back.
    """
    request_start_time = datetime.now()
    current_app.logger.info(
        f"Bulk INSERT request received on /bulk at {request_start_time.isoformat()}"
    )
    
    results = []
    overall_status_code = 200 

    # 1. Validate Request
    if not request.is_json:
        current_app.logger.error("Bulk request content type is not application/json")
        return jsonify(
            {"status": "error", "message": "Request header 'Content-Type' must be 'application/json'"}
        ), 415

    try:
        records_list = request.get_json()
        if not isinstance(records_list, list):
            current_app.logger.error(f"Received data is not a list (Type: {type(records_list)}).")
            return jsonify(
                {"status": "error", "message": "Invalid payload format: body must be a JSON array."}
            ), 400
    except Exception as e:
        current_app.logger.error(f"Failed to parse incoming JSON for bulk request: {e}")
        return jsonify(
            {"status": "error", "message": "Failed to parse request body as JSON array."}
        ), 400

    current_app.logger.info(f"Received {len(records_list)} records in bulk request.")

    if not records_list:
        return jsonify({"status": "success", "message": "Received empty list, no action taken.", "results": []}), 200

    records_prepared_for_insert = 0 # Count records successfully prepared and added to session
    records_with_validation_errors = 0 # Count records that failed pre-DB validation

    for index, record_dict in enumerate(records_list):
        if not isinstance(record_dict, dict):
            current_app.logger.warning(f"Item at index {index} is not a dictionary. Skipping.")
            results.append({
                "original_index": index, "status": "error", "action": "skipped_invalid_format",
                "message": "Item was not a valid JSON object."})
            records_with_validation_errors += 1
            continue

        record_log_id = f"batch_idx_{index}_{record_dict.get('FechaOperacion', 'NO_DATE')}_{record_dict.get('HoraOperacion', 'NO_HOUR')}_{record_dict.get('Gerencia', 'NO_GERENCIA')}"
        
        fecha_op_str = record_dict.get("FechaOperacion")
        hora_op_val = record_dict.get("HoraOperacion")
        gerencia = record_dict.get("Gerencia")
        sistema_val = record_dict.get("Sistema", "UNK")

        record_key_for_response = {"FechaOperacion": fecha_op_str, "HoraOperacion": hora_op_val, "Gerencia": gerencia}

        if fecha_op_str is None or hora_op_val is None or gerencia is None:
            msg = "Record missing required key fields (FechaOperacion, HoraOperacion, Gerencia)."
            current_app.logger.warning(f"{msg} for record {record_log_id}.")
            results.append({"original_index": index, "record_key": record_key_for_response,
                            "status": "error", "action": "skipped_missing_keys", "message": msg})
            records_with_validation_errors += 1
            continue

        try:
            fecha_op = date.fromisoformat(fecha_op_str)
            hora = int(str(hora_op_val))
            if not (0 <= hora <= 24):
                raise ValueError("Hour must be between 0 and 24")
        except (ValueError, TypeError) as conv_err:
            msg = f"Invalid key format for FechaOperacion (expected YYYY-MM-DD) or HoraOperacion: {conv_err}."
            current_app.logger.warning(f"{msg} for record {record_log_id}. Data: {record_dict}")
            results.append({"original_index": index, "record_key": record_key_for_response,
                            "status": "error", "action": "skipped_invalid_key_format", "message": msg})
            records_with_validation_errors += 1
            continue
        
        pk_tuple = (fecha_op, hora, gerencia)

        # Prepare record for insert
        try:
            current_app.logger.info(f"Preparing record {pk_tuple} (from {record_log_id}) for INSERT.")
            new_record = DemandRecord(
                FechaOperacion=fecha_op, HoraOperacion=hora, Gerencia=gerencia,
                Demanda=record_dict.get("Demanda"), Generacion=record_dict.get("Generacion"),
                Pronostico=record_dict.get("Pronostico"), Enlace=record_dict.get("Enlace"),
                Sistema=sistema_val,
            )
            db.session.add(new_record) # Add to session, but not committed yet
            results.append({"original_index": index, "record_key": record_key_for_response,
                            "status": "pending_insert", "action": "insert_queued",
                            "message": f"Record {pk_tuple} queued for insert."})
            records_prepared_for_insert += 1
        except Exception as e_prepare: 
            msg = f"Error preparing record {pk_tuple} (from {record_log_id}) for insert: {e_prepare}"
            current_app.logger.exception(msg)
            results.append({"original_index": index, "record_key": record_key_for_response,
                            "status": "error", "action": "prepare_failed", "message": msg})
            records_with_validation_errors += 1 # This counts as a validation/preparation error

    # Attempt to commit if records were successfully prepared
    if records_prepared_for_insert > 0:
        try:
            db.session.commit()
            current_app.logger.info(f"Bulk INSERT: Successfully committed {records_prepared_for_insert} records.")
            overall_status_code = 201 # HTTP 201 Created

            for res_item in results:
                if res_item.get("action") == "insert_queued":
                    res_item["status"] = "success"
                    res_item["action"] = "inserted"
                    res_item["message"] = res_item["message"].replace("queued for insert", "inserted successfully")
        
        except IntegrityError as ie:
            db.session.rollback()
            current_app.logger.warning(f"Bulk INSERT FAILED: IntegrityError (likely duplicate record). Rolling back. Details: {ie}")
            overall_status_code = 409 # HTTP 409 Conflict
            for res_item in results:
                if res_item.get("action") == "insert_queued": # Update only those that were about to be inserted
                    res_item["status"] = "error"
                    res_item["action"] = "insert_failed_duplicate"
                    res_item["message"] = f"Record {res_item.get('record_key')} failed to insert (likely duplicate). Batch rolled back."
            # Add a general batch failure message to results
            results.append({"original_index": -1, "status": "error", "action": "batch_commit_failed_integrity_error",
                            "message": "Batch insert failed due to a unique constraint violation (duplicate record). All queued records in this batch were rolled back."})

        except SQLAlchemyError as db_commit_err:
            db.session.rollback()
            current_app.logger.exception(f"Bulk INSERT FAILED: SQLAlchemyError during commit. Rolling back. Details: {db_commit_err}")
            overall_status_code = 500
            for res_item in results:
                if res_item.get("action") == "insert_queued":
                    res_item["status"] = "error"
                    res_item["action"] = "insert_failed_db_error"
                    res_item["message"] = f"Record {res_item.get('record_key')} failed to insert due to database error. Batch rolled back."
            results.append({"original_index": -1, "status": "error", "action": "batch_commit_failed_sqla_error",
                            "message": "Batch insert failed due to a database error during commit. All queued records in this batch were rolled back."})
        
        except Exception as e_commit:
            db.session.rollback()
            current_app.logger.exception(f"Bulk INSERT FAILED: Unexpected error during commit. Rolling back. Details: {e_commit}")
            overall_status_code = 500
            for res_item in results:
                if res_item.get("action") == "insert_queued":
                    res_item["status"] = "error"
                    res_item["action"] = "insert_failed_unexpected_error"
                    res_item["message"] = f"Record {res_item.get('record_key')} failed to insert due to an unexpected error. Batch rolled back."
            results.append({"original_index": -1, "status": "error", "action": "batch_commit_failed_other_error",
                            "message": "Batch insert failed due to an unexpected error during commit. All queued records in this batch were rolled back."})
    
    elif records_with_validation_errors > 0 and records_prepared_for_insert == 0:
        current_app.logger.warning("All records in batch had pre-processing/validation errors. No DB commit attempted.")
        overall_status_code = 400 # Bad Request as all items failed validation
    elif not records_list: # Handled at the top, but as a safeguard
        overall_status_code = 200
    else: # No records prepared, no validation errors (e.g. list was empty after filtering invalid format items)
        current_app.logger.info("Bulk request: No records were queued for insert (e.g., all items might have failed initial format validation).")
        overall_status_code = 200 if not records_with_validation_errors else 400


    request_end_time = datetime.now()
    duration = (request_end_time - request_start_time).total_seconds()
    
    final_successful_count = len([r for r in results if r.get("status") == "success" and r.get("action") == "inserted"])
    # Total records received minus successful ones. This includes validation errors and rolled_back inserts.
    final_failed_count = len(records_list) - final_successful_count

    current_app.logger.info(
        f"Bulk INSERT request finished in {duration:.2f} seconds. Attempted: {records_prepared_for_insert}, Validation Errors: {records_with_validation_errors}, Successfully Inserted: {final_successful_count}"
    )
    
    final_response = {
        "summary": {
            "total_records_received": len(records_list),
            "records_attempted_insert": records_prepared_for_insert,
            "records_with_validation_errors": records_with_validation_errors,
            "successfully_inserted": final_successful_count,
            "failed_or_rolled_back": final_failed_count - records_with_validation_errors # Subtract pre-db errors from this count
        },
        "results": results
    }
    return jsonify(final_response), overall_status_code

@demanda_bp.route('/demanda_sin', methods=['GET'])
def sin_demand_route():
    return get_sin_demand_comparison()

@demanda_bp.route('/demanda_comparison', methods=['GET'])
def get_demanda_comparison_data():
    return get_demanda_aggregates_for_comparison_dates()