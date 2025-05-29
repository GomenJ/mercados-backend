from datetime import datetime, date
from flask import Blueprint, request, jsonify, current_app
from ...models.demand_record import DemandRecord 
from typing import Optional
from ... import db

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
        if not (0 <= hora <= 23):
            raise ValueError("Hour must be between 0 and 23")
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