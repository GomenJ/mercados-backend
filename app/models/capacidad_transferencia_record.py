from sqlalchemy.sql import func
from .. import db

class CapacidadTransferenciaRecord(db.Model):
    __tablename__ = "CapacidadTransferencia"

    Id = db.Column(db.Integer, primary_key=True) # Assuming IDENTITY(1,1) is handled by DB
    Sistema = db.Column(db.String(3), nullable=False)
    FechaOperacion = db.Column(db.Date, nullable=False)
    Enlace = db.Column(db.String(32), nullable=False)
    Horario = db.Column(db.Integer, nullable=False) # Check expected range (e.g., 1-24 or 0-23)
    CapTransDisImpComMwh = db.Column(db.Integer, nullable=False)
    CapResImpEneInadMwh = db.Column(db.Integer, nullable=False)
    CapResImpConfMWh = db.Column(db.Integer, nullable=False)
    CapAbsTransDisImpMWh = db.Column(db.Integer, nullable=False)
    CapTransDisExpComMwh = db.Column(db.Integer, nullable=False)
    # NOTE: Original SQL had 'CapResExpEneInaMwh'. If this was a typo and should be 'Inad', adjust here.
    CapResExpEneInaMwh = db.Column(db.Integer, nullable=False)
    CapResExpConfMwh = db.Column(db.Integer, nullable=False)
    CapAbsTransDisExpMwh = db.Column(db.Integer, nullable=False)

    # Assuming you want creation/update timestamps like DemandRecord
    FechaCreacion = db.Column(
        db.DateTime, server_default=func.now(), nullable=True # Nullable if they might not exist in older data
    )
    FechaActualizacion = db.Column(
        db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=True
    )

    # Optional: Add a unique constraint if the combination of business keys should be unique
    # __table_args__ = (
    #     db.UniqueConstraint('Sistema', 'FechaOperacion', 'Enlace', 'Horario', name='uq_capacidad_transferencia_key'),
    # )

    def __init__(self, **kwargs):
        # Standard __init__ using SQLAlchemy's keyword argument mapping
        super().__init__(**kwargs)

    def __repr__(self):
        return f"<CapacidadTransferenciaRecord id={self.Id} {self.Sistema} {self.FechaOperacion} {self.Enlace} H{self.Horario}>"

    # You could add data_is_different or update_from_dict methods here if needed for future endpoints,
    # but for this insert-only endpoint, they aren't strictly required.

# Ensure CapacidadTransferenciaRecord is imported in app.py if you're not using `import *`
