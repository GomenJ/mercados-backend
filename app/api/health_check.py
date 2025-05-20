from flask import Blueprint, jsonify, current_app
from .. import db

health_check_bp = Blueprint("health_check", __name__)

@health_check_bp.route("", methods=["GET"])
def health_check():
    # You could add a db ping here if desired
    try:
        db.session.execute(db.text('SELECT 1'))
        db_status = "ok"
    except Exception as e:
        db_status = "error"
        current_app.logger.error(f"Health check DB error: {e}")
    return jsonify({"status": "ok", "database": db_status}), 200
