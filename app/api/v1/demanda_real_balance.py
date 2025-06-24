# This api handles https://www.cenace.gob.mx/Paginas/SIM/Reportes/EstimacionDemandaReal.aspx
from flask import Blueprint, request, jsonify, current_app
from datetime import datetime
from ...models.demanda_real_balance_record import DemandaRealBalanceRecord
from ... import db  # Assuming db is initialized in app/__init__.py
from sqlalchemy.exc import IntegrityError
from ...services.demanda_real_balance_service import (
    create_demanda_records,
    PublicationDateExistsError, # Assuming it's defined in the service file
    DataValidationError,        # Assuming it's defined in the service file
    get_yearly_peak_demand_comparison
) 

demanda_real_balance_bp = Blueprint('demanda_real_balance', __name__)

# http://127.0.0.1:5001/api/v1/demanda_real_balance/yearly_peak_demand_comparison
# --- NEW: Endpoint for CapacidadTransferencia Batch Inserts ---
@demanda_real_balance_bp.route('', methods=['POST'])
def add_demanda_records_batch():
    """
    Receives a list of demanda records and attempts to insert them.
    Checks if FechaPublicacion already exists before inserting.
    """
    data = request.get_json()

    if not isinstance(data, list):
        return jsonify({"message": "Invalid input: Expected a list of records."}), 400
    if not data:
        return jsonify({"message": "Invalid input: List cannot be empty."}), 400
    try:
        # The service still returns the list of created ORM objects
        created_records_objects = create_demanda_records(data)
        
        # --- MODIFICATION START ---
        # We no longer serialize the full list.
        # We just use the count from the returned objects.
        count_created = len(created_records_objects) if created_records_objects else 0
        
        return jsonify({
            "message": f"{count_created} records created successfully."
        }), 201
        # --- MODIFICATION END ---

    except PublicationDateExistsError as e:
        return jsonify({"message": str(e)}), 409
    except DataValidationError as e:
        return jsonify({"message": "Data validation error.", "errors": e.errors}), 400
    except IntegrityError as e:
        db.session.rollback()
        current_app.logger.error(f"Integrity error in add_demanda_records_batch: {str(e.orig)}", exc_info=True)
        return jsonify({"message": "Database integrity error during batch insert.", "error_detail": str(e.orig)}), 409
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Unexpected error in add_demanda_records_batch: {str(e)}", exc_info=True)
        return jsonify({"message": "An unexpected error occurred.", "error": str(e)}), 500

# @demanda_real_balance_bp.route('', methods=['POST'])
# def add_demanda_records_batch():
#     """
#     Receives a list of demanda records and attempts to insert them.
#     Checks if FechaPublicacion already exists before inserting.
#     """
#     data = request.get_json()

#     if not isinstance(data, list):
#         return jsonify({"message": "Invalid input: Expected a list of records."}), 400
#     if not data:
#         return jsonify({"message": "Invalid input: List cannot be empty."}), 400
#     try:
#         # The service still returns the list of created ORM objects
#         created_records_objects = create_demanda_records(data)
        
#         # --- MODIFICATION START ---
#         # We no longer serialize the full list.
#         # We just use the count from the returned objects.
#         count_created = len(created_records_objects) if created_records_objects else 0
        
#         return jsonify({
#             "message": f"{count_created} records created successfully."
#         }), 201
#         # --- MODIFICATION END ---

#     except PublicationDateExistsError as e:
#         return jsonify({"message": str(e)}), 409
#     except DataValidationError as e:
#         return jsonify({"message": "Data validation error.", "errors": e.errors}), 400
#     except IntegrityError as e:
#         db.session.rollback()
#         current_app.logger.error(f"Integrity error in add_demanda_records_batch: {str(e.orig)}", exc_info=True)
#         return jsonify({"message": "Database integrity error during batch insert.", "error_detail": str(e.orig)}), 409
#     except Exception as e:
#         db.session.rollback()
#         current_app.logger.error(f"Unexpected error in add_demanda_records_batch: {str(e)}", exc_info=True)
#         return jsonify({"message": "An unexpected error occurred.", "error": str(e)}), 500

@demanda_real_balance_bp.route("/yearly_peak_demand_comparison", methods=["GET"])
def get_yearly_peak_demand_data():
    """
    Retrieves the daily peak hourly demand, comparing the current year-to-date
    against the same period in the previous year.
    """
    try:
        current_app.logger.info("Received request for yearly peak demand comparison.")
        
        # Call the service to get the processed data
        comparison_data = get_yearly_peak_demand_comparison()

        current_app.logger.info("Successfully fetched yearly peak demand comparison data.")
        return jsonify(comparison_data), 200

    except Exception as e:
        current_app.logger.exception(
            f"Error fetching yearly peak demand comparison data: {e}"
        )
        return jsonify({"status": "error", "message": "Failed to fetch yearly peak demand data."}), 500
