# app/services/demanda_real_balance_service.py
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from .. import db 
from ..models.demanda_real_balance_record import DemandaRealBalanceRecord

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


def create_demanda_records(data_list):
    """
    Processes a list of demanda records, checks for existing FechaPublicacion,
    and inserts them into the database.

    Args:
        data_list (list): A list of dictionaries, where each dictionary
                          represents a record to be inserted.

    Raises:
        PublicationDateExistsError: If any record with the given FechaPublicacion
                                    (from the first item in data_list) already exists.
        DataValidationError: If data parsing or validation fails.
        IntegrityError: If a database integrity constraint is violated during commit.
                        (e.g. unique constraint on specific record combination)
    Returns:
        list: A list of the created DemandaRealBalanceRecord objects.
    """
    if not data_list:
        raise DataValidationError(errors=["Input data list cannot be empty."])

    # --- 1. Check FechaPublicacion from the first record ---
    # Assuming FechaPublicacion is consistent for the batch or the first one is representative.
    try:
        fecha_publicacion_str = data_list[0].get("FechaPublicacion")
        if not fecha_publicacion_str:
            raise DataValidationError(errors=["FechaPublicacion is missing in the first record."])
        
        # Convert to date object for querying
        fecha_publicacion_to_check = datetime.strptime(fecha_publicacion_str, "%Y-%m-%d").date()
    except ValueError:
        raise DataValidationError(errors=[f"Invalid FechaPublicacion format: '{fecha_publicacion_str}'. Expected YYYY-MM-DD."])

    existing_record = DemandaRealBalanceRecord.query.filter_by(FechaPublicacion=fecha_publicacion_to_check).first()
    if existing_record:
        raise PublicationDateExistsError(fecha_publicacion=fecha_publicacion_to_check)

    # --- 2. Process and create new records ---
    new_records_instances = []
    validation_errors = []

    for index, item_data in enumerate(data_list):
        try:
            parsed_data = {
                "DiaOperacion": datetime.strptime(item_data.get("DiaOperacion"), "%d/%m/%Y").date(),
                "Sistema": item_data.get("Sistema"),
                "Area": item_data.get("Area"),
                "Hora": int(item_data.get("Hora")),
                "Generacion_MWh": Decimal(str(item_data.get("Generacion_MWh"))) if item_data.get("Generacion_MWh") is not None else None,
                "Importacion_Total_MWh": Decimal(str(item_data.get("Importacion_Total_MWh"))) if item_data.get("Importacion_Total_MWh") is not None else None,
                "Exportacion_Total_MWh": Decimal(str(item_data.get("Exportacion_Total_MWh"))) if item_data.get("Exportacion_Total_MWh") is not None else None,
                "Intercambio_Neto_Entre_Gerencias_MWh": Decimal(str(item_data.get("Intercambio_Neto_Entre_Gerencias_MWh"))) if item_data.get("Intercambio_Neto_Entre_Gerencias_MWh") not in [None, '---'] else None,
                "Estimacion_Demanda_Por_Balance_MWh": Decimal(str(item_data.get("Estimacion_Demanda_Por_Balance_MWh"))) if item_data.get("Estimacion_Demanda_Por_Balance_MWh") is not None else None,
                "Liq": int(item_data.get("Liq")),
                "FechaPublicacion": datetime.strptime(item_data.get("FechaPublicacion"), "%Y-%m-%d").date(),
            }
            # Handle optional FechaCreacion and FechaActualizacion if present in input
            # If you want the DB to ALWAYS set these, remove them from parsed_data
            if "FechaCreacion" in item_data and item_data["FechaCreacion"]:
                parsed_data["FechaCreacion"] = datetime.fromisoformat(item_data["FechaCreacion"])
            if "FechaActualizacion" in item_data and item_data["FechaActualizacion"]:
                parsed_data["FechaActualizacion"] = datetime.fromisoformat(item_data["FechaActualizacion"])

            # Basic validation for required fields (SQLAlchemy nullable=False will also catch this at DB level)
            for key in ["Sistema", "Area", "Hora", "Liq", "DiaOperacion", "FechaPublicacion"]:
                if parsed_data.get(key) is None:
                    validation_errors.append(f"Record {index+1}: Missing required field '{key}'.")
            
            if not validation_errors: # Only create instance if basic parsing passed for this item
                record = DemandaRealBalanceRecord(**parsed_data)
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


def get_yearly_peak_demand_comparison():
    """
    Calculates the peak hourly demand for each day in the current year-to-date
    and compares it with the equivalent period in the previous year.

    The peak demand for a day is defined as the maximum sum of hourly demand 
    across all areas (excluding 'BCA' and 'BCS').
    """
    
    # 1. Determine Dynamic Date Ranges
    today = date.today()
    current_year = today.year
    
    # Current Year Period (CY): Jan 1st of current year to today
    cy_start_date = date(current_year, 1, 1)
    cy_end_date = today

    # Previous Year Period (PY): Jan 1st of previous year to the equivalent date last year
    py_start_date = date(current_year - 1, 1, 1)
    py_end_date = today.replace(year=today.year - 1)


    # 2. Define the Parameterized SQL Query
    # This query uses a subquery to first calculate the total demand for each hour of each day,
    # and then the outer query finds the maximum hourly value for each day.
    sql_query = text("""
        SELECT
            DiaOperacion,
            MAX(SumaEstimacion) AS MaxSumaPorHora
        FROM (
            SELECT
                DiaOperacion,
                Hora,
                SUM(Estimacion_Demanda_Por_Balance_MWh) AS SumaEstimacion
            FROM DemandaRealBalance
            WHERE
                Sistema NOT IN ('BCA', 'BCS')
                AND Liq = 0
                AND (
                    -- Range for the current year-to-date
                    (DiaOperacion BETWEEN :cy_start AND :cy_end)
                    OR
                    -- Range for the same period in the previous year
                    (DiaOperacion BETWEEN :py_start AND :py_end)
                )
            GROUP BY
                DiaOperacion,
                Hora
        ) AS Subconsulta
        GROUP BY
            DiaOperacion
        ORDER BY
            DiaOperacion;
    """)

    # 3. Bind parameters and Execute the Query
    bound_query = sql_query.bindparams(
        cy_start=cy_start_date,
        cy_end=cy_end_date,
        py_start=py_start_date,
        py_end=py_end_date
    )
    
    results = db.session.execute(bound_query).fetchall()

    # 4. Process and Structure the Data
    current_year_data = []
    previous_year_data = []

    for row in results:
        record = {
            # Format date as a string for JSON compatibility
            "Fecha": row.DiaOperacion.isoformat(),
            # Use a more descriptive name and convert Decimal to float
            "MaxDemandaHoraria_MWh": float(row.MaxSumaPorHora) if row.MaxSumaPorHora is not None else None
        }
        
        # Separate records into current and previous year lists
        if row.DiaOperacion.year == current_year:
            current_year_data.append(record)
        else:
            previous_year_data.append(record)

    # 5. Return the structured data
    return {
        "currentYearData": current_year_data,
        "previousYearData": previous_year_data,
        "dateRanges": {
            "currentYear": {"start": cy_start_date.isoformat(), "end": cy_end_date.isoformat()},
            "previousYear": {"start": py_start_date.isoformat(), "end": py_end_date.isoformat()}
        }
    }