from marshmallow import fields, validate, ValidationError, validates_schema
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, auto_field
from decimal import Decimal # Import Decimal

# Import db, ma from app factory and models
from . import ma, db # Assuming ma is initialized in __init__.py
from .models import (
    DemandRecord,
    BasePnxRecord,
    PndMdaRecord,
    PmlMdaRecord,
    PmlMtrRecord,
    PndMtrRecord,
    CapacidadTransferencia
)

# --- Helper for Decimal conversion ---
# Marshmallow handles Decimal conversion well, but explicit definition can add clarity/validation
class SafeDecimal(fields.Decimal):
    def _deserialize(self, value, attr, data, **kwargs):
        if value == '' or value is None: # Handle empty strings as None
            return None
        try:
            # Allow string or number input, specify precision context if needed
            return super()._deserialize(value, attr, data, **kwargs)
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid decimal value: {value}") from e

# --- Demand Record Schema ---
class DemandRecordSchema(SQLAlchemyAutoSchema):
    # Explicitly declare fields if you need specific validation or types
    HoraOperacion = fields.Integer(
        required=True, validate=validate.Range(min=0, max=23)
    )
    FechaOperacion = fields.Date(required=True)
    Gerencia = fields.String(required=True, validate=validate.Length(max=50))
    Sistema = fields.String(required=True, validate=validate.Length(max=10))

    # Allow nullable integer fields
    Demanda = fields.Integer(allow_none=True)
    Generacion = fields.Integer(allow_none=True)
    Pronostico = fields.Integer(allow_none=True)
    Enlace = fields.Integer(allow_none=True)

    class Meta:
        model = DemandRecord
        load_instance = True # Create/update model instances upon loading
        sqla_session = db.session # Needed for load_instance=True with relationships/commits
        # Exclude fields managed by the server from loading
        exclude = ("id", "FechaCreacion", "FechaModificacion")
        # Alternatively, use dump_only for specific fields:
        # dump_only = ("id", "FechaCreacion", "FechaModificacion")


# --- Base Schema for PND/PML records ---
class BasePnxSchema(SQLAlchemyAutoSchema):
    # Explicitly declare fields for validation and decimal handling
    Sistema = fields.String(required=True, validate=validate.Length(equal=3))
    Fecha = fields.Date(required=True)
    # Adjust Hora range if needed (e.g., 1-24 vs 0-23)
    Hora = fields.Integer(required=True, validate=validate.Range(min=0, max=24))
    Clave = fields.String(required=True, validate=validate.Length(max=20))

    # Use SafeDecimal helper or standard fields.Decimal
    # `as_string=True` is important if you expect numbers wrapped in quotes in JSON
    PML = SafeDecimal(places=2, as_string=True, allow_none=True)
    Energia = SafeDecimal(places=2, as_string=True, allow_none=True)
    Congestion = SafeDecimal(places=2, as_string=True, allow_none=True)
    Perdidas = SafeDecimal(places=2, as_string=True, allow_none=True)

    # We don't set 'model' here as it's abstract.
    # We will use this schema directly in the resource, passing the correct model.
    class Meta:
        # model = BasePnxRecord # Cannot set model for abstract class directly
        load_instance = True
        sqla_session = db.session
        # Define fields explicitly to control loading/dumping behavior for primary keys
        fields = ("Sistema", "Fecha", "Hora", "Clave", "PML", "Energia", "Congestion", "Perdidas")
        # If PKs should not be updatable via load:
        # load_only = () # Or specify fields that CAN be loaded
        # dump_only = () # Or specify fields only for output


# --- You could optionally create specific schemas if needed, but often the base is enough ---
# class PndMdaSchema(BasePnxSchema):
#     class Meta(BasePnxSchema.Meta):
#         model = PndMdaRecord

# class PmlMdaSchema(BasePnxSchema):
#     class Meta(BasePnxSchema.Meta):
#         model = PmlMdaRecord
# etc.

class CapacidadTransferenciaSchema(SQLAlchemyAutoSchema):
    # Explicitly define fields requiring specific validation or that are NOT NULL
    # This ensures validation happens before hitting the DB.
    Sistema = fields.String(
        validate=validate.Length(max=3),
        required=True,
        allow_none=False # Explicitly disallow null
    )
    FechaOperacion = fields.Date(required=True, allow_none=False)
    Enlace = fields.String(
        validate=validate.Length(max=32),
        required=True,
        allow_none=False # Explicitly disallow null
    )
    Horario = fields.Integer(
        required=True,
        validate=validate.Range(min=0, max=23), # Assuming 0-23 hour range
        allow_none=False # Explicitly disallow null
    )

    # Add explicit definitions for the non-nullable integer fields
    # This makes the API contract clearer and validates input early.
    CapTransDisImpComMwh = fields.Integer(required=True, allow_none=False)
    CapResImpEneInadMwh = fields.Integer(required=True, allow_none=False)
    CapResImpConfMWh = fields.Integer(required=True, allow_none=False)
    CapAbsTransDisImpMWh = fields.Integer(required=True, allow_none=False)
    CapTransDisExpComMwh = fields.Integer(required=True, allow_none=False)
    CapResExpEneInaMwh = fields.Integer(required=True, allow_none=False)
    CapResExpConfMwh = fields.Integer(required=True, allow_none=False)
    CapAbsTransDisExpMwh = fields.Integer(required=True, allow_none=False)


    # Timestamps are typically managed by the server, so only dump them.
    FechaCreacion = fields.DateTime(dump_only=True)
    FechaActualizacion = fields.DateTime(dump_only=True)


    class Meta:
        model = CapacidadTransferencia # Reference the updated model class
        load_instance = True # Create/update model instances upon loading
        sqla_session = db.session # Needed for load_instance=True

        # Exclude server-managed fields (PK, timestamps) from being loaded
        # Use the *model attribute names* here.
        exclude = ("Id", "FechaCreacion", "FechaActualizacion")

        # Alternatively, if you prefer 'dump_only' over 'exclude':
        # dump_only = ("Id", "FechaCreacion", "FechaActualizacion")
        # Or list all fields explicitly if you don't want to rely on AutoSchema much:
        # fields = (
        #     "Sistema", "FechaOperacion", "Enlace", "Horario",
        #     "CapTransDisImpComMwh", "CapResImpEneInadMwh", # ... all capacity fields ...
        #     "Id", "FechaCreacion", "FechaActualizacion" # Include dump_only fields here too
        # )