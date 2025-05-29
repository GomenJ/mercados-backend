# app/services/pml_aggregation_service.py

from app import db
from datetime import date, timedelta
from sqlalchemy import text, bindparam
from app.models.pml_pnd_records import PmlMdaRecord  # Import the correct model


def get_pml_aggregates_for_comparison_dates():
    """
    Calculates aggregated PML data (average, max, min) for the latest date
    and the date one week prior for Sistema='SIN' and relevant Gerencias.
    """
    # Determine latest_date from the PMLMDA table
    latest_date_obj = db.session.query(db.func.max(PmlMdaRecord.Fecha)).scalar()

    if not latest_date_obj:
        return {"latest_day_records": [], "previous_week_day_records": []}

    # Determine previous_week_date
    previous_week_date_obj = latest_date_obj - timedelta(days=6)

    # Ensure previous_week_date actually exists in the table
    # This prevents errors if there's a data gap
    previous_week_date_exists = db.session.query(PmlMdaRecord.Fecha)\
        .filter(PmlMdaRecord.Fecha == previous_week_date_obj).limit(1).first()

    dates_to_query = [latest_date_obj]
    if previous_week_date_exists:
        dates_to_query.append(previous_week_date_obj)
    else:
        # Log a warning if the previous week's data isn't found
        db.session.info(
            f"No data found for {previous_week_date_obj.isoformat()} "
            f"while querying PML aggregates. Only fetching data for {latest_date_obj.isoformat()}."
        )


    sql_query = text("""
        SELECT
            cn.CentroControlRegional AS Gerencia,
            p.Fecha,
            AVG(p.PML) AS Promedio_PML,
            MAX(p.PML) AS Maximo_PML,
            MIN(p.PML) AS Minimo_PML
        FROM
            [InfoMercado].[dbo].[PMLMDA] p
        INNER JOIN
            [InfoMercado].[dbo].[CatalogoNodos] cn ON p.Clave = cn.Clave
        WHERE
            p.Fecha IN :dates_to_query
            AND p.Sistema = 'SIN'
            AND cn.CentroControlRegional != 'No aplica'
        GROUP BY
            cn.CentroControlRegional,
            p.Fecha
        ORDER BY
            cn.CentroControlRegional,
            p.Fecha;
    """)

    # Use bindparams with expanding=True for the IN clause
    bound_query = sql_query.bindparams(
        bindparam('dates_to_query', value=dates_to_query, expanding=True)
    )

    results = db.session.execute(bound_query).fetchall()

    latest_day_records = []
    previous_week_day_records = []

    for row in results:
        record = {
            "Gerencia": row.Gerencia,
            "Fecha": row.Fecha.isoformat(),  # Format date as string
            "Promedio_PML": float(row.Promedio_PML) if row.Promedio_PML is not None else None,
            "Maximo_PML": float(row.Maximo_PML) if row.Maximo_PML is not None else None,
            "Minimo_PML": float(row.Minimo_PML) if row.Minimo_PML is not None else None,
        }
        if row.Fecha == latest_date_obj:
            latest_day_records.append(record)
        elif row.Fecha == previous_week_date_obj:
            previous_week_day_records.append(record)

    return {
        "latest_day_records": latest_day_records,
        "previous_week_day_records": previous_week_day_records,
        "latest_date": latest_date_obj.isoformat() if latest_date_obj else None,
        "previous_week_date": previous_week_date_obj.isoformat() if previous_week_date_exists else None # Only include if data exists
    }

