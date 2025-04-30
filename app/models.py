from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from sqlalchemy.schema import UniqueConstraint # Make sure this is imported
from decimal import Decimal, InvalidOperation
from typing import Dict, Any

# This db object will be initialized in app/__init__.py
# We define it here so models can use it.
db = SQLAlchemy()

class DemandRecord(db.Model):
    __tablename__ = "Demanda"

    id = db.Column(db.Integer, primary_key=True)
    FechaOperacion = db.Column(db.Date, nullable=False)
    HoraOperacion = db.Column(db.Integer, nullable=False) # 0-23
    Gerencia = db.Column(db.String(20), nullable=False)
    Demanda = db.Column(db.Integer, nullable=True)
    Generacion = db.Column(db.Integer, nullable=True)
    Pronostico = db.Column(db.Integer, nullable=True)
    Enlace = db.Column(db.Integer, nullable=True)
    Sistema = db.Column(db.String(10), nullable=False)

    FechaCreacion = db.Column(
        db.DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    FechaModificacion = db.Column(
        db.DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "FechaOperacion",
            "HoraOperacion",
            "Gerencia",
            name="uq_demand_record_fecha_hora_gerencia",
        ),
    )

    # --- Explicit Constructor ---
    def __init__(self, **kwargs):
        """
        Explicit constructor using SQLAlchemy's recommended approach.
        It accepts keyword arguments matching column names.
        """
        super(DemandRecord, self).__init__(**kwargs)

    def __repr__(self):
        return f"<DemandRecord id={self.id} {self.FechaOperacion} H{self.HoraOperacion} {self.Gerencia}>"

    def data_is_different(self, data_dict: Dict[str, Any]) -> bool:
        """Checks if relevant data fields differ from the incoming dict."""
        # Use .get with a default that won't match None if field is missing
        if self.Demanda != data_dict.get("Demanda"):
            return True
        if self.Generacion != data_dict.get("Generacion"):
            return True
        if self.Pronostico != data_dict.get("Pronostico"):
            return True
        if self.Enlace != data_dict.get("Enlace"):
            return True
        return False

# --- Abstract Base Class for common structure ---
class BasePnxRecord(db.Model):
    __abstract__ = True

    Sistema = db.Column(db.String(3), primary_key=True)
    Fecha = db.Column(db.Date, primary_key=True)
    Hora = db.Column(db.Integer, primary_key=True) # Expecting 1-24 (or 0-23?) Check range
    Clave = db.Column(db.String(20), primary_key=True)
    PML = db.Column(db.Numeric(10, 2), nullable=True)
    Energia = db.Column(db.Numeric(10, 2), nullable=True)
    Congestion = db.Column(db.Numeric(10, 2), nullable=True)
    Perdidas = db.Column(db.Numeric(10, 2), nullable=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.Sistema} {self.Fecha} H{self.Hora} {self.Clave}>"

    @staticmethod
    def _to_decimal_or_none(value):
        """Helper to safely convert value to Decimal or None."""
        if value is None or value == '':
            return None
        try:
            # Convert numeric types directly, strings need explicit conversion
            if isinstance(value, (int, float)):
                 return Decimal(value)
            elif isinstance(value, str):
                 return Decimal(value)
            elif isinstance(value, Decimal):
                 return value # Already a decimal
            else:
                 # Attempt conversion for other types if necessary, log if unknown
                 # print(f"Attempting Decimal conversion for unknown type: {type(value)}")
                 return Decimal(value)
        except (InvalidOperation, ValueError, TypeError):
            # Log this error if needed
            # current_app.logger.warning(f"Could not convert value '{value}' (type: {type(value)}) to Decimal.")
            return None # Treat conversion errors as None

    # --- Methods for comparison/update, using the helper ---

    def data_is_different(self, data_dict: Dict[str, Any]) -> bool:
        """Checks if relevant data fields differ from the incoming dict."""
        if self.PML != self._to_decimal_or_none(data_dict.get("PML")):
            return True
        if self.Energia != self._to_decimal_or_none(data_dict.get("Energia")):
            return True
        if self.Congestion != self._to_decimal_or_none(data_dict.get("Congestion")):
            return True
        if self.Perdidas != self._to_decimal_or_none(data_dict.get("Perdidas")):
            return True
        return False

    def update_from_dict(self, data_dict: Dict[str, Any]):
        """Updates the record's fields from a dictionary."""
        self.PML = self._to_decimal_or_none(data_dict.get("PML"))
        self.Energia = self._to_decimal_or_none(data_dict.get("Energia"))
        self.Congestion = self._to_decimal_or_none(data_dict.get("Congestion"))
        self.Perdidas = self._to_decimal_or_none(data_dict.get("Perdidas"))


# --- Concrete Model Classes ---
class PndMdaRecord(BasePnxRecord):
    __tablename__ = "PNDMDA"

class PmlMdaRecord(BasePnxRecord):
    __tablename__ = "PMLMDA"

class PmlMtrRecord(BasePnxRecord):
    __tablename__ = "PMLMTR"

class PndMtrRecord(BasePnxRecord):
    __tablename__ = "PNDMTR"


class CapacidadTransferencia(db.Model):
    __tablename__ = "CapacidadTransferencia"

    # Define columns based on SQL table
    Id = db.Column(db.Integer, primary_key=True) # Assuming IDENTITY(1,1) maps to autoincrement PK
    Sistema = db.Column(db.String(3), nullable=True) # Assuming nullable based on common patterns, adjust if needed
    FechaOperacion = db.Column(db.Date, nullable=False) # Assuming non-nullable date
    Enlace = db.Column(db.String(32), nullable=True)
    Horario = db.Column(db.Integer, nullable=False) # Assuming non-nullable hour
    CapTransDisImpComMwh = db.Column(db.Integer, nullable=True) # Explicit name mapping just in case
    CapResImpEneInadMwh = db.Column(db.Integer, nullable=True)
    CapResImpConfMWh = db.Column(db.Integer, nullable=True)
    CapAbsTransDisImpMWh = db.Column(db.Integer, nullable=True)
    CapTransDisExpComMwh = db.Column(db.Integer, nullable=True)
    CapResExpEneInaMwh = db.Column(db.Integer, nullable=True)
    CapResExpConfMwh = db.Column(db.Integer, nullable=True) # Typo in SQL 'MWH'? Assumed MWh like others
    CapAbsTransDisExpMwh = db.Column(db.Integer, nullable=True)

    # Using DateTime for creation timestamp consistency with other models
    # SQL definition has `Date`, adjust if strictly required
    FechaCreacion = db.Column(
        db.DateTime(timezone=True),
        server_default=func.now(), # Use func.now() for DateTime
        nullable=False
    )

    FechaActualizacion = db.Column(
        db.DateTime, # Matches DATETIME type in SQL
        onupdate=func.now(), # Automatically set on update by the database (if supported/configured)
                             # Alternatively, manage this in your application logic
        nullable=True # Matches DATETIME (implicitly nullable)
    )

    # If you MUST use Date as per SQL:
    # FechaCreacion = db.Column(db.Date, server_default=func.current_date(), nullable=False)


    # Optional: Add a Unique Constraint if needed, e.g.:
    # __table_args__ = (
    #     db.UniqueConstraint('Sistema', 'FechaOperacion', 'Enlace', 'Horario', name='uq_capacidad_transferencia_key'),
    # )
   # Optional: Add a Unique Constraint if needed (adjust name if necessary)
    __table_args__ = (
        db.UniqueConstraint('Sistema', 'FechaOperacion', 'Enlace', 'Horario', name='uq_capacidad_transferencia_key'),
    )
    def __repr__(self):
        return f"<CapacidadTransferencia ID={self.ID} Fecha={self.FechaOperacion} Horario={self.Horario} Enlace={self.Enlace}>"