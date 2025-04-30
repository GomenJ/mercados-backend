from flask import request, current_app
from flask_restful import Resource
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError

# Import db, models, schemas
from .. import db
from ..models import DemandRecord
from ..schemas import DemandRecordSchema

# Instantiate schemas
demand_schema = DemandRecordSchema()
demands_schema = DemandRecordSchema(many=True) # For potential future GET list endpoint


class DemandResource(Resource):
    """Resource for single Demand records."""

    def post(self):
        """
        Create a new DemandRecord or update an existing one (Upsert).
        Checks based on FechaOperacion, HoraOperacion, Gerencia.
        """
        json_data = request.get_json()
        if not json_data:
            return {"status": "error", "message": "No input data provided"}, 400

        try:
            # Validate and load data
            # Note: load_instance=True means this returns a DemandRecord object
            # but it's not yet added to the session.
            loaded_data = demand_schema.load(json_data)

            # Check if record exists based on business key
            existing_record = DemandRecord.query.filter_by(
                FechaOperacion=loaded_data.FechaOperacion,
                HoraOperacion=loaded_data.HoraOperacion,
                Gerencia=loaded_data.Gerencia
            ).first()

            if existing_record:
                # --- UPDATE or CONFLICT ---
                if existing_record.data_is_different(json_data): # Use original json_data for comparison method
                    current_app.logger.info(f"Record found ({existing_record.id}). Data differs. Updating.")
                    # Update existing record fields from loaded data
                    existing_record.Demanda = loaded_data.Demanda
                    existing_record.Generacion = loaded_data.Generacion
                    existing_record.Pronostico = loaded_data.Pronostico
                    existing_record.Enlace = loaded_data.Enlace
                    # No need to update keys or Sistema if they are part of the unique key
                    db.session.commit()
                    current_app.logger.info(f"UPDATE successful for record id {existing_record.id}")
                    return {
                        "status": "success",
                        "action": "updated",
                        "message": f"Record id {existing_record.id} updated.",
                        "record": demand_schema.dump(existing_record) # Return updated record
                    }, 200
                else:
                    # --- CONFLICT ---
                    current_app.logger.info(f"CONFLICT (Identical) for record id {existing_record.id}. No changes.")
                    return {
                        "status": "conflict",
                        "action": "none",
                        "message": "Record already exists with identical data.",
                        "record": demand_schema.dump(existing_record) # Return existing record
                    }, 409 # Conflict
            else:
                # --- INSERT ---
                current_app.logger.info(f"Record not found for {loaded_data.FechaOperacion} H{loaded_data.HoraOperacion} {loaded_data.Gerencia}. Inserting.")
                # `loaded_data` is already a detached DemandRecord instance due to load_instance=True
                new_record = loaded_data
                db.session.add(new_record)
                db.session.commit()
                current_app.logger.info(f"INSERT successful for {new_record.FechaOperacion} H{new_record.HoraOperacion} {new_record.Gerencia} (ID: {new_record.id})")
                return {
                        "status": "success",
                        "action": "inserted",
                        "message": "Record inserted successfully.",
                        "record": demand_schema.dump(new_record) # Return newly created record
                    }, 201 # Created

        except ValidationError as err:
             # Error handler should catch this, but explicit handling is okay too
             return {"status": "error", "message": "Input validation failed", "errors": err.messages}, 400
        except IntegrityError as err:
             db.session.rollback()
             # Error handler should catch this
             current_app.logger.error(f"Database integrity error during demand upsert: {err}")
             return {"status": "error", "message": "Database conflict, possibly duplicate record."}, 409
        except Exception as e:
            db.session.rollback()
            current_app.logger.exception(f"Error processing demand record: {e}")
            # Generic error handler should catch this
            return {"status": "error", "message": "An internal server error occurred."}, 500