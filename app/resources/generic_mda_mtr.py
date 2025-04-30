from flask import request, current_app
from flask_restful import Resource, abort
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError, DBAPIError
from datetime import date

# Import db, models, schemas
from .. import db
from ..models import PndMdaRecord, PmlMdaRecord, PmlMtrRecord, PndMtrRecord
from ..schemas import BasePnxSchema # Use the base schema

# Mapping from URL data_type to Model Class
# Consider moving this to a config or utility file if it grows large
DATA_TYPE_MODELS = {
    "pnd_mda": PndMdaRecord,
    "pml_mda": PmlMdaRecord,
    "pml_mtr": PmlMtrRecord,
    "pnd_mtr": PndMtrRecord,
}

# Instantiate base schema for batch operations
# We'll pass the correct model to load/dump dynamically if needed,
# but SQLAlchemyAutoSchema with load_instance=True works well here.
base_pnx_batch_schema = BasePnxSchema(many=True)
base_pnx_single_schema = BasePnxSchema() # For potential future single record ops

class GenericBatchResource(Resource):
    """Handles batch inserts for PND/PML records."""

    def post(self, data_type):
        """
        Receives a batch of records for the specified data_type.
        Performs inserts only, assuming client checked for existing date data.
        """
        model_key = data_type.lower()
        ModelClass = DATA_TYPE_MODELS.get(model_key)

        if not ModelClass:
            # Use abort() for cleaner handling of standard HTTP errors within Resources
            abort(404, status="error", message=f"Invalid data type specified: '{data_type}'. Valid types are: {list(DATA_TYPE_MODELS.keys())}")

        json_data = request.get_json()
        if not isinstance(json_data, list):
            return {"status": "error", "message": "Request body must be a JSON list."}, 400

        if not json_data:
             return {"status": "success", "summary": {"total_records_received": 0, "inserted": 0, "errors": 0}, "errors": []}, 200 # Empty list is valid


        summary = {
            "total_records_received": len(json_data),
            "inserted": 0,
            "database_errors": 0 # Count records that failed during DB commit
        }
        # Note: Marshmallow handles validation errors before this point

        try:
            # Validate the entire batch and get list of dictionaries or model instances
            # load_instance=True in schema means this will be a list of ModelClass instances
            loaded_instances = base_pnx_batch_schema.load(
                json_data,
                # You might need to tell the schema which model to use if it can't infer
                # This might require creating specific schemas or adjusting the base schema usage
            )

            if not loaded_instances:
                 # Should not happen if json_data was not empty and validation passed, but check anyway
                 current_app.logger.warning(f"No valid instances loaded for '{model_key}' despite non-empty input.")
                 return {"status": "error", "message": "Batch validation failed, no records to insert."}, 400


            # Add all validated instances to the session
            db.session.add_all(loaded_instances)
            db.session.commit()

            summary["inserted"] = len(loaded_instances)
            current_app.logger.info(f"Commit successful for '{model_key}' batch. Inserted: {summary['inserted']}")
            return {"status": "success", "summary": summary}, 201 # 201 Created for successful batch insert

        except ValidationError as err:
             # This catches validation errors for the whole batch
             summary['database_errors'] = summary['total_records_received'] # Assume all failed if validation fails here
             current_app.logger.error(f"Validation failed for '{model_key}' batch: {err.messages}")
             # Return detailed validation errors
             return {"status": "error", "message": "Batch validation failed", "errors": err.messages, "summary": summary}, 400

        except (IntegrityError, DBAPIError) as db_err:
            db.session.rollback()
            summary['database_errors'] = len(loaded_instances) if 'loaded_instances' in locals() else summary['total_records_received']
            summary['inserted'] = 0
            current_app.logger.exception(f"Database error during '{model_key}' batch commit: {db_err}")
            # Provide a less specific error to the client
            error_message = "Database conflict or error during batch insert. Batch rolled back."
            if isinstance(db_err, IntegrityError):
                 error_message = "Database conflict (e.g., duplicate key) during batch insert. Batch rolled back."

            return {"status": "error", "message": error_message, "summary": summary}, 500 # Internal Server Error or 409? 500 seems safer for batch failure.

        except Exception as e:
            db.session.rollback()
            summary['database_errors'] = len(loaded_instances) if 'loaded_instances' in locals() else summary['total_records_received']
            summary['inserted'] = 0
            current_app.logger.exception(f"Unexpected error during '{model_key}' batch processing: {e}")
            return {"status": "error", "message": "An internal server error occurred during batch processing.", "summary": summary}, 500


class DataExistenceResource(Resource):
    """Checks if data exists for a given type and date."""

    def get(self, data_type, fecha):
        """Checks existence based on data_type and fecha (YYYY-MM-DD)."""
        model_key = data_type.lower()
        ModelClass = DATA_TYPE_MODELS.get(model_key)

        if not ModelClass:
            abort(404, status="error", message=f"Invalid data type specified: '{data_type}'.")

        try:
            parsed_date = date.fromisoformat(fecha)
        except ValueError:
             return {"status": "error", "message": f"Invalid fecha format: '{fecha}'. Expected YYYY-MM-DD."}, 400

        try:
            # Efficiently check for existence using scalar() or exists()
            # exists() might be slightly more efficient as it translates to SQL EXISTS
            # Note: primary key fields are likely indexed, making this query fast.
            # We query for one of the PK fields.
            exists_query = db.session.query(ModelClass.Sistema).filter(ModelClass.Fecha == parsed_date).limit(1)
            exists = db.session.query(exists_query.exists()).scalar()

            current_app.logger.info(f"Check result for {model_key} on {fecha}: {'Exists' if exists else 'Does not exist'}")
            return {"exists": bool(exists)}, 200

        except Exception as e:
            current_app.logger.exception(f"Database error during existence check for {model_key} on {fecha}: {e}")
            return {"status": "error", "message": "Database query failed during check."}, 500