from sqlalchemy.sql import func
from .. import db
class ImportExportLiquidadaRecord(db.Model):
    __tablename__ = "ImpExpLiquidada"

    Id = db.Column(db.Integer, primary_key=True)  # Assuming SERIAL is handled by DB
    DiaOperacion = db.Column(db.Date, nullable=False)
    Fecha_Publicacion = db.Column(db.Date, nullable=False)
    Sistema = db.Column(db.String(3), nullable=False)
    Liquidacion = db.Column(db.Integer, nullable=False)
    EnlaceInternacional = db.Column(db.String(50), nullable=False)
    HoraOperacion = db.Column(db.Integer, nullable=False)  # Check expected range (e.g., 1-24 or 0-23)
    
    Importacion_Comercial_MWh = db.Column(db.Numeric(5, 2), nullable=False)
    Importacion_Pago_Energia_Inadvertida_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Importacion_Confiabilidad_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Importacion_CIL_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Importacion_Total_MWh = db.Column(db.Numeric(8, 5), nullable=False)

    Exportacion_Comercial_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Exportacion_Cobro_Energia_Inadvertida_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Exportacion_Confiabilidad_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Exportacion_CIL_MWh = db.Column(db.Numeric(8, 5), nullable=False)
    Exportacion_Total_MWh = db.Column(db.Numeric(8, 5), nullable=False)

    Fecha_Creacion = db.Column(
        db.DateTime, server_default=func.now(), nullable=True
    )
    Fecha_Actualizacion = db.Column(
        db.DateTime, server_default=func.now(), onupdate=func.now(), nullable=True
    )

    __table_args__ = (
        db.CheckConstraint(
            "Importacion_Comercial_MWh + Importacion_Pago_Energia_Inadvertida_MWh + "
            "Importacion_Confiabilidad_MWh + Importacion_CIL_MWh = Importacion_Total_MWh",
            name="chk_importacion_total"
        ),
        # Add any other constraints or indexes as needed
    )

    def __init__(self, **kwargs):
        # Standard __init__ using SQLAlchemy's keyword argument mapping
        super().__init__(**kwargs)

    def __repr__(self):
        return f"<ImportExportLiquidadaRecord id={self.Id} {self.Sistema} {self.Fecha_Publicacion} {self.EnlaceInternacional} H{self.HoraOperacion}>"