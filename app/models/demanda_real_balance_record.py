from sqlalchemy.sql import func
from .. import db

class DemandaRealBalanceRecord(db.Model):
    __tablename__ = "DemandaRealBalance"
    # Create the model for this scheme with the data table 
    Id = db.Column(db.Integer, primary_key=True) # Assuming IDENTITY(1,1) is handled by DB
    DiaOperacion = db.Column(db.Date, nullable=False) # The date the data in the CSV pertains to (e.g., FechaReferenciaLiq - 42 days)
    Sistema = db.Column(db.String(49), nullable=False)
    Area = db.Column(db.String(3), nullable=False)
    Hora = db.Column(db.Integer, nullable=False) # Check expected range (e.g., 1-24 or 0-23)
    Generacion_MWh = db.Column(db.Numeric(17, 5), nullable=True)
    Importacion_Total_MWh = db.Column(db.Numeric(17, 5), nullable=True)
    Exportacion_Total_MWh = db.Column(db.Numeric(17, 5), nullable=True)
    Intercambio_Neto_Entre_Gerencias_MWh = db.Column(db.Numeric(17, 5), nullable=True) # Store '---' as NULL
    Estimacion_Demanda_Por_Balance_MWh = db.Column(db.Numeric(17, 5), nullable=True)
    Liq = db.Column(db.Integer, nullable=False) # Liquidation version (0, 1, 2, or 3) relative to FechaReferenciaLiq
    FechaPublicacion = db.Column(db.Date, nullable=False) # The "anchor" or "current" date for this Liq 0-3 set. For Liq 0, FechaOperacion == FechaReferenciaLiq.
    FechaCreacion = db.Column(
        db.DateTime, server_default=func.now(), nullable=True # Nullable if they might not exist in older data
    )
    FechaActualizacion = db.Column(
        db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=True
    )
    # Optional: Add a unique constraint if the combination of business keys should be unique
    __table_args__ = (
        db.UniqueConstraint('DiaOperacion', 'Sistema', 'Area', 'Hora', 'Liq', 'FechaPublicacion', name='UQ_DemandaRealBalance_OperacionLiqRefUnica'),
        db.CheckConstraint('Liq IN (0, 1, 2, 3)', name='CK_LiqRange'),
    )

    def __init__(self, **kwargs):
        # Standard __init__ using SQLAlchemy's keyword argument mapping
        super().__init__(**kwargs)
    def __repr__(self):
        return f"<DemandaRealBalanceRecord id={self.Id} {self.Sistema} {self.DiaOperacion} {self.Area} H{self.Hora}>"


