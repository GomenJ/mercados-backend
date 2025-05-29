from app import db
from datetime import date
from sqlalchemy import text, bindparam
from app.models.pml_pnd_records import PndMdaRecord
# Ensure you have 'calendar' if you use the previous_year_end logic with it
import calendar

def get_daily_average_pnd_by_clave_split_years(): # Renamed for clarity
    """
    Calculates the daily average PML for specific claves and splits them
    into current and previous year lists.
    The periods cover current year up to latest_date
    and the previous year up to the same month/day as latest_date.
    """
    claves = [
        "MONTERREY", "VDM NORTE", "PUEBLA", "AGUASCALIENTES", "LAGUNA",
        "NAVOJOA", "QUERETARO", "IRAPUATO", "ZACAPU", "SAN LUIS POTOSI",
        "PIEDRAS NEGRAS"
    ]

    latest_date_obj = db.session.query(db.func.max(PndMdaRecord.Fecha)).scalar()

    if not latest_date_obj:
        return {"currentYearData": [], "previousYearData": []} # Return structure consistently

    current_year_num = latest_date_obj.year
    previous_year_num = current_year_num - 1

    current_year_start = date(current_year_num, 1, 1)
    current_year_end = latest_date_obj

    previous_year_start = date(previous_year_num, 1, 1)
    try:
        previous_year_end = date(previous_year_num, latest_date_obj.month, latest_date_obj.day)
    except ValueError:
        # Handle Feb 29 on non-leap previous year
        _, last_day_of_month = calendar.monthrange(previous_year_num, latest_date_obj.month)
        previous_year_end = date(previous_year_num, latest_date_obj.month, last_day_of_month)

    sql_query = text("""
        SELECT
            Fecha,
            AVG(PML) as average_PML
        FROM PNDMDA
        WHERE Clave IN :keys_list
        AND (
            (Fecha >= :current_year_start AND Fecha <= :current_year_end) OR
            (Fecha >= :previous_year_start AND Fecha <= :previous_year_end)
        )
        GROUP BY Fecha
        ORDER BY Fecha;
    """)

    bound_query = sql_query.bindparams(
        bindparam('keys_list', value=claves, expanding=True),
        current_year_start=current_year_start,
        current_year_end=current_year_end,
        previous_year_start=previous_year_start,
        previous_year_end=previous_year_end
    )

    results = db.session.execute(bound_query).fetchall()

    current_year_data = []
    previous_year_data = []

    for row in results:
        # The row.Fecha is already a date object from SQLAlchemy if the column type is Date/Datetime
        record_year = row.Fecha.year
        formatted_record = {
            'Fecha': row.Fecha.strftime('%Y-%m-%d'),
            'average_PML': float(row.average_PML) if row.average_PML is not None else None
        }
        if record_year == current_year_num:
            current_year_data.append(formatted_record)
        elif record_year == previous_year_num:
            previous_year_data.append(formatted_record)

    return {
        "currentYearData": current_year_data,
        "previousYearData": previous_year_data,
        "currentYear": current_year_num, # Optional: send year numbers for labels
        "previousYear": previous_year_num # Optional: send year numbers for labels
    }