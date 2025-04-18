from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declared_attr  # Import this
from sqlalchemy.sql import func
from typing import Dict, Any

# from sqlalchemy.schema import UniqueConstraint  # Import this
from decimal import Decimal, InvalidOperation

db = SQLAlchemy()


class DemandRecord(db.Model):
    __tablename__ = "Demanda"  # Use your actual table name

    id = db.Column(
        db.Integer, primary_key=True
    )  # SQLAlchemy typically handles autoincrement

    # --- Business Key Fields (Not PK, but should be unique together) ---
    FechaOperacion = db.Column(db.Date, nullable=False)
    HoraOperacion = db.Column(db.Integer, nullable=False)  # 0-23
    Gerencia = db.Column(db.String(50), nullable=False)  # Adjust size if needed

    # --- Data fields ---
    Demanda = db.Column(db.Integer, nullable=True)
    Generacion = db.Column(db.Integer, nullable=True)
    Pronostico = db.Column(db.Integer, nullable=True)
    Enlace = db.Column(db.Integer, nullable=True)
    Sistema = db.Column(db.String(10), nullable=False)

    # --- Timestamps ---
    FechaCreacion = db.Column(
        db.DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    FechaModificacion = db.Column(
        db.DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # --- Unique Constraint ---
    # Enforce uniqueness on the combination of the business key fields
    __table_args__ = (
        db.UniqueConstraint(
            "FechaOperacion",
            "HoraOperacion",
            "Gerencia",
            name="uq_demand_record_fecha_hora_gerencia",
        ),
        # Add other constraints or indexes here if needed
    )

    # --- Explicit Constructor ---
    def __init__(self, **kwargs):
        """
        Explicit constructor using SQLAlchemy's recommended approach.
        It accepts keyword arguments matching column names.
        """
        # This calls the parent class's __init__ and handles mapping kwargs
        # to the defined columns. It's the standard way to override.
        super(DemandRecord, self).__init__(**kwargs)

    def __repr__(self):
        # Include id in representation now
        return f"<DemandRecord id={self.id} {self.FechaOperacion} H{self.HoraOperacion} {self.Gerencia}>"

    def data_is_different(self, data_dict: Dict[str, Any]) -> bool:
        """Checks if relevant data fields differ from the incoming dict."""
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
    __abstract__ = True  # Important: Makes this a base class, not a table itself

    # --- Common Business Key Fields ---
    # Mark the columns that together uniquely identify a row as primary_key=True
    Sistema = db.Column(db.String(3), primary_key=True)
    Fecha = db.Column(db.Date, primary_key=True)
    Hora = db.Column(db.Integer, primary_key=True)  # Expecting 1-24
    Clave = db.Column(db.String(20), primary_key=True)
    # --- Common Data fields ---
    PML = db.Column(db.Numeric(10, 2), nullable=True)
    Energia = db.Column(db.Numeric(10, 2), nullable=True)
    Congestion = db.Column(db.Numeric(10, 2), nullable=True)
    Perdidas = db.Column(db.Numeric(10, 2), nullable=True)

    # --- Common Timestamps ---
    # Use @declared_attr for columns that might depend on the subclass or need defaults
    # @declared_attr
    # def FechaCreacion(cls):
    #     return db.Column(
    #         db.DateTime(timezone=True), server_default=func.now(), nullable=False
    #     )
    #
    # @declared_attr
    # def FechaModificacion(cls):
    #     return db.Column(
    #         db.DateTime(timezone=True),
    #         server_default=func.now(),
    #         onupdate=func.now(),
    #         nullable=False,
    #     )

    # Define the Unique Constraint structure once, name will be set in subclasses
    # @declared_attr
    # def __table_args__(cls):
    #     # Generate a unique constraint name based on the table name
    #     constraint_name = f"uq_{cls.__tablename__}_record"
    #     return (
    #         UniqueConstraint("Sistema", "Fecha", "Hora", "Clave", name=constraint_name),
    #     )

    # --- Common Methods ---
    # These methods are inherited by all subclasses
    def __init__(self, **kwargs):
        # Standard way to handle keyword args in SQLAlchemy models
        super().__init__(**kwargs)

    def __repr__(self):
        # Use self.__class__.__name__ to get the actual model name (PndMdaRecord, etc.)
        return f"<{self.__class__.__name__} {self.Sistema} {self.Fecha} H{self.Hora} {self.Clave}>"

    def _to_decimal_or_none(self, value):
        """Helper to safely convert value to Decimal or None."""
        if value is None:
            return None
        try:
            # Handle potential string representations from JSON edge cases
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            # Log or handle conversion error if needed, returning None for comparison
            # app.logger.warning(f"Could not convert value {value} to Decimal in comparison.")
            return None  # Treat conversion errors as None for comparison

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
        # Add other updatable fields if necessary


# --- Concrete Model Classes (Minimal Definitions) ---


class PndMdaRecord(BasePnxRecord):
    # __tablename__ = "pnd_mda"
    __tablename__ = "PNDMDA"
    # __table_args__ are inherited and name is generated automatically
    # __table_args__ = (
    #     UniqueConstraint("Sistema", "Fecha", "Hora", "Clave", name="uq_pnd_mda_record"),
    #     # Add other table-specific args like indexes here if needed
    # )


class PmlMdaRecord(BasePnxRecord):
    # __tablename__ = "pml_mda"
    __tablename__ = "PMLMDA"
    # __table_args__ are inherited
    # __table_args__ = (
    #     UniqueConstraint("Sistema", "Fecha", "Hora", "Clave", name="uq_pml_mda_record"),
    # )


class PmlMtrRecord(BasePnxRecord):
    # __tablename__ = "pml_mtr"
    __tablename__ = "PMLMTR"
    # __table_args__ are inherited
    # __table_args__ = (
    #     UniqueConstraint("Sistema", "Fecha", "Hora", "Clave", name="uq_pml_mtr_record"),
    # )


class PndMtrRecord(BasePnxRecord):
    # __tablename__ = "pnd_mtr"
    __tablename__ = "PNDMTR"
    # __table_args__ are inherited
    # __table_args__ = ()
