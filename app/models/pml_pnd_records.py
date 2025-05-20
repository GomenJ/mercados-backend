from typing import Dict, Any
# from sqlalchemy.schema import UniqueConstraint  # Import this
from decimal import Decimal, InvalidOperation
from .. import db

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

# --- Concrete Model Classes (Minimal Definitions) ---
class PndMdaRecord(BasePnxRecord):
    __tablename__ = "PNDMDA"


class PmlMdaRecord(BasePnxRecord):
    __tablename__ = "PMLMDA"

class PmlMtrRecord(BasePnxRecord):
    __tablename__ = "PMLMTR"

class PndMtrRecord(BasePnxRecord):
    __tablename__ = "PNDMTR"