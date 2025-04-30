from flask import request, current_app
from flask_restful import Resource, abort
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError, DBAPIError

# Import db, model, schema
from .. import db
from ..models import CapacidadTransferencia
from ..schemas import CapacidadTransferenciaSchema

# Instantiate schema
capacidad_schema = CapacidadTransferenciaSchema()
capacidades_schema = CapacidadTransferenciaSchema(many=True) # For potential future GET list endpoint

class CapacidadTransferenciaResource(Resource):
    """Resource for managing CapacidadTransferencia records."""

    def post(self):
        """
        Creates a new CapacidadTransferencia record.
        Assumes simple insert; add duplicate checking if needed.
        """
        json_data = request.get_json()
        if not json_data:
            return {"status": "error", "message": "No input data provided"}, 400

        try:
            # Validate and load data into a model instance
            new_record = capacidad_schema.load(json_data)

            # Optional: Check for duplicates based on business key if necessary
            # existing = CapacidadTransferencia.query.filter_by(...).first()
            # if existing:
            #     # Handle update or conflict (return 409)
            #     return {"status": "conflict", "message": "Record already exists."}, 409

            db.session.add(new_record)
            db.session.commit()

            current_app.logger.info(f"CapacidadTransferencia record created (Id: {new_record.Id})")
            return {
                "status": "success",
                "message": "Record created successfully.",
                "record": capacidad_schema.dump(new_record) # Return the created record
            }, 201 # Created

        except ValidationError as err:
             # Error handler should catch this
             current_app.logger.error(f"Validation failed for CapacidadTransferencia: {err.messages}")
             return {"status": "error", "message": "Input validation failed", "errors": err.messages}, 400
        except IntegrityError as err:
             db.session.rollback()
             # Error handler should catch this
             current_app.logger.error(f"Database integrity error creating CapacidadTransferencia: {err}")
             return {"status": "error", "message": "Database conflict or integrity violation."}, 409 # Conflict
        except (DBAPIError, Exception) as e:
            db.session.rollback()
            current_app.logger.exception(f"Error creating CapacidadTransferencia record: {e}")
            # Generic error handler should catch this
            return {"status": "error", "message": "An internal server error occurred."}, 500

    # Add GET, PUT, DELETE methods here as needed
    # def get(self, record_id=None):
    #     if record_id:
    #         # Get single record by ID
    #         record = CapacidadTransferencia.query.get_or_404(record_id)
    #         return capacidad_schema.dump(record)
    #     else:
    #         # Get list of records (add pagination!)
    #         records = CapacidadTransferencia.query.all()
    #         return capacidades_schema.dump(records)