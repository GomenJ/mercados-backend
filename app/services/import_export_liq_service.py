# app/services/demanda_real_balance_service.py
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from .. import db 
from ..models.import_export_liq_record import ImportExportLiquidadaRecord

class PublicationDateExistsError(Exception):
    """Custom exception raised when data for a FechaPublicacion already exists."""
    def __init__(self, fecha_publicacion, message="Data for FechaPublicacion already exists."):
        self.fecha_publicacion = fecha_publicacion
        self.message = f"{message}: {fecha_publicacion}"
        super().__init__(self.message)

class DataValidationError(Exception):
    """Custom exception for data validation errors during processing."""
    def __init__(self, errors, message="Data validation failed."):
        self.errors = errors
        self.message = message
        super().__init__(self.message)


def create_import_export_records(data_list):
    """
    Processes a list of import_exports_liq records, checks for existing Fecha_Publicacion,
    and inserts them into the database.

    Args:
        data_list (list): A list of dictionaries, where each dictionary
                          represents a record to be inserted.

    Raises:
        PublicationDateExistsError: If any record with the given Fecha_Publicacion
                                    (from the first item in data_list) already exists.
        DataValidationError: If data parsing or validation fails.
        IntegrityError: If a database integrity constraint is violated during commit.
                        (e.g. unique constraint on specific record combination)
    Returns:
        list: A list of the created DemandaRealBalanceRecord objects.
    """
    if not data_list:
        raise DataValidationError(errors=["Input data list cannot be empty."])

    # --- 1. Check Fecha_Publicacion from the first record ---
    # Assuming Fecha_Publicacion is consistent for the batch or the first one is representative.
    try:
        fecha_publicacion_str = data_list[0].get("Fecha_Publicacion")
        if not fecha_publicacion_str:
            raise DataValidationError(errors=["Fecha_Publicacion is missing in the first record."])
        
        # Convert to date object for querying
        fecha_publicacion_to_check = datetime.strptime(fecha_publicacion_str, "%Y-%m-%d").date()
    except ValueError:
        raise DataValidationError(errors=[f"Invalid Fecha_Publicacion format: '{fecha_publicacion_str}'. Expected YYYY-MM-DD."])

    existing_record = ImportExportLiquidadaRecord.query.filter_by(Fecha_Publicacion=fecha_publicacion_to_check).first()
    if existing_record:
        raise PublicationDateExistsError(fecha_publicacion=fecha_publicacion_to_check)

    # --- 2. Process and create new records ---
    new_records_instances = []
    validation_errors = []

    for index, item_data in enumerate(data_list):
        try:
            parsed_data = {
                "DiaOperacion": datetime.strptime(item_data.get("DiaOperacion"), "%Y-%m-%d").date(),
                "Fecha_Publicacion": datetime.strptime(item_data.get("Fecha_Publicacion"), "%Y-%m-%d").date(),
                "Sistema": item_data.get("Sistema"),
                "Liquidacion": item_data.get("Liquidacion"),
                "EnlaceInternacional": item_data.get("EnlaceInternacional"),
                "HoraOperacion": int(item_data.get("HoraOperacion")),
                "Importacion_Comercial_MWh": Decimal(str(item_data.get("Importacion_Comercial_MWh"))) if item_data.get("Importacion_Comercial_MWh") is not None else None,
                "Importacion_Pago_Energia_Inadvertida_MWh": Decimal(str(item_data.get("Importacion_Pago_Energia_Inadvertida_MWh"))) if item_data.get("Importacion_Pago_Energia_Inadvertida_MWh") is not None else None,
                "Importacion_Confiabilidad_MWh": Decimal(str(item_data.get("Importacion_Confiabilidad_MWh"))) if item_data.get("Importacion_Confiabilidad_MWh") is not None else None,
                "Importacion_CIL_MWh": Decimal(str(item_data.get("Importacion_CIL_MWh"))) if item_data.get("Importacion_CIL_MWh") is not None else None,
                "Importacion_Total_MWh": Decimal(str(item_data.get("Importacion_Total_MWh"))) if item_data.get("Importacion_Total_MWh") is not None else None,
                "Exportacion_Comercial_MWh": Decimal(str(item_data.get("Exportacion_Comercial_MWh"))) if item_data.get("Exportacion_Comercial_MWh") is not None else None,
                "Exportacion_Cobro_Energia_Inadvertida_MWh": Decimal(str(item_data.get("Exportacion_Cobro_Energia_Inadvertida_MWh"))) if item_data.get("Exportacion_Cobro_Energia_Inadvertida_MWh") is not None else None,
                "Exportacion_Confiabilidad_MWh": Decimal(str(item_data.get("Exportacion_Confiabilidad_MWh"))) if item_data.get("Exportacion_Confiabilidad_MWh") is not None else None,
                "Exportacion_CIL_MWh": Decimal(str(item_data.get("Exportacion_CIL_MWh"))) if item_data.get("Exportacion_CIL_MWh") is not None else None,
                "Exportacion_Total_MWh": Decimal(str(item_data.get("Exportacion_Total_MWh"))) if item_data.get("Exportacion_Total_MWh") is not None else None,
            }
            # Handle optional Fecha_Creacion and Fecha_Actualizacion if present in input
            # If you want the DB to ALWAYS set these, remove them from parsed_data
            if "Fecha_Creacion" in item_data and item_data["Fecha_Creacion"]:
                parsed_data["Fecha_Creacion"] = datetime.fromisoformat(item_data["Fecha_Creacion"])
            if "Fecha_Actualizacion" in item_data and item_data["Fecha_Actualizacion"]:
                parsed_data["Fecha_Actualizacion"] = datetime.fromisoformat(item_data["Fecha_Actualizacion"])

            # Basic validation for required fields (SQLAlchemy nullable=False will also catch this at DB level)
            for key in ["Sistema", "Liquidacion", "EnlaceInternacional", "DiaOperacion", "Fecha_Publicacion"]:
                if parsed_data.get(key) is None:
                    validation_errors.append(f"Record {index+1}: Missing required field '{key}'.")
            
            if not validation_errors: # Only create instance if basic parsing passed for this item
                record = ImportExportLiquidadaRecord(**parsed_data)
                new_records_instances.append(record)

        except (ValueError, TypeError, InvalidOperation) as e:
            validation_errors.append(f"Record {index+1}: Error parsing data - {str(e)}. Data: {item_data}")
        except Exception as e: # Catch any other unexpected error during item processing
            validation_errors.append(f"Record {index+1}: Unexpected error - {str(e)}. Data: {item_data}")
    if validation_errors:
        raise DataValidationError(errors=validation_errors)

    if not new_records_instances: # Should be caught by empty data_list or all items failing validation
        raise DataValidationError(errors=["No valid records to insert after processing."])

    try:
        db.session.add_all(new_records_instances)
        db.session.commit()
        return new_records_instances
    except IntegrityError as e:
        db.session.rollback()
        # This could be due to the UQ_DemandaRealBalance_OperacionLiqRefUnica for a specific record
        raise IntegrityError(f"Database integrity error: {str(e.orig)}", params=e.params, orig=e.orig)
    except Exception as e:
        db.session.rollback()
        raise Exception(f"An unexpected error occurred during database commit: {str(e)}")

