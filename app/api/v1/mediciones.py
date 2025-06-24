from flask import Blueprint, request, jsonify, current_app
from ...services.mediciones_service import get_last_mediciones_per_day

mediciones_bp = Blueprint('mediciones', __name__)

@mediciones_bp.route('', methods=['GET'])
def get_mediciones():
    """
    Retrieves the daily peak hourly demand, comparing the current year-to-date
    against the same period in the previous year.
    """
    try:
        current_app.logger.info("Received request for mediciones.")
        
        # Call the service to get the processed data
        mediciones =  get_last_mediciones_per_day()

        current_app.logger.info("Successfully fetched mediciones data.")
        return mediciones, 200

    except Exception as e:
        current_app.logger.exception(
            f"Error fetching mediciones data: {e}"
        )
        return jsonify({"status": "error", "message": "Failed to fetch mediciones data."}), 500

