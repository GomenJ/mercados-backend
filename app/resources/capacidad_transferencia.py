from flask import request, current_app, jsonify # Added jsonify for potential custom error structure if needed
from flask_restful import Resource # Removed abort as it wasn't used directly here
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError, DBAPIError

# Import db, model, schema
from .. import db
from ..models import CapacidadTransferencia
from ..schemas import CapacidadTransferenciaSchema

# --- Schema Instances ---
# Instance for handling a SINGLE object (useful for GET by ID, PUT, DELETE)
capacidad_schema_single = CapacidadTransferenciaSchema()
# Instance for handling a LIST of objects (for POST bulk create and GET list)
capacidades_schema_many = CapacidadTransferenciaSchema(many=True)


# --- Resource Definition ---
class CapacidadTransferenciaResource(Resource):
    """
    Resource for managing CapacidadTransferencia records.
    POST method now handles bulk creation from a JSON array.
    """

    def post(self):
        """
        Creates multiple new CapacidadTransferencia records from a JSON array.
        """
        json_data = request.get_json()

        # --- Input Validation ---
        if not json_data:
            return {"status": "error", "message": "No input data provided"}, 400

        # Ensure input is a list (JSON array)
        if not isinstance(json_data, list):
             return {"status": "error", "message": "Input must be a list (JSON array) of objects"}, 400

        # Handle empty list input specifically if needed (or let validation handle it)
        if not json_data:
             # Depending on requirements, an empty list might be valid (noop) or an error
             return {"status": "error", "message": "Input list cannot be empty"}, 400

        try:
            # --- Deserialization and Validation (using many=True schema) ---
            # This expects a list of dictionaries and returns a list of model instances
            new_records = capacidades_schema_many.load(json_data)

            # Check if load resulted in an empty list (e.g., if input was valid but empty schema)
            if not new_records:
                 return {"status": "error", "message": "Validated data is empty."}, 400 # Or appropriate code

            # --- Database Operation ---
            db.session.add_all(new_records) # Use add_all for bulk efficiency
            db.session.commit()

            # --- Success Response ---
            num_created = len(new_records)
            current_app.logger.info(f"{num_created} CapacidadTransferencia records created successfully.")
            return {
                "status": "success",
                "message": f"{num_created} records created successfully.",
                # Return the list of created records (including IDs/timestamps)
                "records": capacidades_schema_many.dump(new_records)
            }, 201 # 201 Created

        except ValidationError as err:
            # Log the validation errors
            current_app.logger.error(f"Validation failed for CapacidadTransferencia bulk insert: {err.messages}")
            # Return validation errors (422 Unprocessable Entity is often preferred over 400)
            return {"status": "error", "message": "Input validation failed", "errors": err.messages}, 422
        except IntegrityError as err:
            db.session.rollback()
            # Log the specific integrity error
            current_app.logger.error(f"Database integrity error during CapacidadTransferencia bulk insert: {err}")
            # Determine if it's a duplicate or other constraint violation
            # Providing more specific error messages based on err.orig might be possible
            return {"status": "error", "message": "Database conflict or integrity violation. Ensure records are unique based on constraints."}, 409 # 409 Conflict
        except (DBAPIError, Exception) as e:
            db.session.rollback()
            # Log the generic exception
            current_app.logger.exception(f"Error during CapacidadTransferencia bulk insert: {e}")
            return {"status": "error", "message": "an internal server error occurred during data processing."}, 500

    # --- Other methods (GET, PUT, DELETE) would likely use ---
    # --- capacidad_schema_single or capacidades_schema_many as appropriate ---

    # Example GET methods (adapt as needed)
    def get(self, record_id=None):
        if record_id:
            # Get single record by ID
            record = CapacidadTransferencia.query.get(record_id) # Use get for PK lookup
            if not record:
                 return {"status": "error", "message": "Record not found"}, 404
            return {"status": "success", "record": capacidad_schema_single.dump(record)}, 200
        else:
            # Get list of records (add pagination in a real application!)
            records = CapacidadTransferencia.query.all()
            return {"status": "success", "records": capacidades_schema_many.dump(records)}, 200

# Remember to register this resource correctly in your app factory or main app file:
# from .resources.capacidad_transferencia import CapacidadTransferenciaResource
# api.add_resource(CapacidadTransferenciaResource,
#                  '/api/capacidades-transferencia',          # Route for POST (bulk) and GET (list)
#                  '/api/capacidades-transferencia/<int:record_id>') # Route for GET (single), PUT, DELETE