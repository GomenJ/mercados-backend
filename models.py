from flask_sqlalchemy import SQLAlchemy

from sqlalchemy.sql import func
from typing import Dict, Any

db = SQLAlchemy()


class DemandRecord(db.Model):
    __tablename__ = "Demanda"  # Use your actual table name

    # --- Primary Key ---
    # Use a standard auto-incrementing integer ID as the primary key
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
