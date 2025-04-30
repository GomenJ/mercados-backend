from flask import jsonify
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError, DBAPIError
from werkzeug.exceptions import HTTPException, UnprocessableEntity

# You'll register these handlers in app/__init__.py

def handle_validation_error(e):
    """Handle Marshmallow validation errors."""
    return jsonify({"status": "error", "message": "Validation error", "errors": e.messages}), 400

def handle_integrity_error(e):
    """Handle database integrity errors (e.g., unique constraint)."""
    # Log the detailed error for debugging
    # current_app.logger.error(f"Database Integrity Error: {e.orig}")
    # Provide a generic message to the client
    return jsonify({"status": "error", "message": "Database conflict or integrity violation."}), 409 # 409 Conflict is often suitable

def handle_dbapi_error(e):
    """Handle lower-level database errors."""
    # Log the error
    # current_app.logger.error(f"Database API Error: {e}")
    return jsonify({"status": "error", "message": "Database connection or query error."}), 500

def handle_not_found_error(e):
    """Handle Werkzeug NotFound errors (e.g., invalid data_type)."""
    return jsonify({"status": "error", "message": e.description}), 404

def handle_unprocessable_entity(e):
    """Handle cases where the entity is syntactically correct but semantically wrong."""
    # Often used by webargs/Marshmallow if parsing succeeds but validation fails later
    messages = getattr(e, 'data', {}).get('messages', 'Unprocessable entity.')
    return jsonify({"status": "error", "message": "Unprocessable entity", "errors": messages}), 422

def handle_generic_http_exception(e):
    """Handle other Werkzeug HTTP errors."""
    return jsonify({"status": "error", "message": e.description}), e.code

def handle_generic_exception(e):
    """Handle unexpected server errors."""
    # Log the full exception
    # current_app.logger.exception(f"An unexpected error occurred: {e}")
    return jsonify({"status": "error", "message": "An internal server error occurred."}), 500

# Dictionary of error handlers to register
error_handlers = {
    ValidationError: handle_validation_error,
    IntegrityError: handle_integrity_error,
    DBAPIError: handle_dbapi_error, # Catch lower-level DB connection/query issues
    #NotFound: handle_not_found_error, # Flask-RESTful might handle 404s differently
    UnprocessableEntity: handle_unprocessable_entity,
    HTTPException: handle_generic_http_exception, # Catch general HTTP errors last
    Exception: handle_generic_exception, # Catch any other exception
}