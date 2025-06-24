from flask import request, jsonify
from sqlalchemy import text, bindparam
from datetime import date, timedelta
from .. import db 
from ..models.demand_record import DemandRecord

def get_sin_demand_comparison():
    """
    Calculates the peak hourly demand for the 'SIN' system for each day
    in the current year-to-date and compares it with the equivalent period
    in the previous year.

    This endpoint accepts an optional URL parameter 'gerencia' to filter
    the results for a specific management area.

    URL Examples:
    - /api/sin_demand_comparison (for all gerencias)
    - /api/sin_demand_comparison?gerencia=Noroeste (filtered for 'Noroeste')
    """
    try:
        # 1. Get Optional 'gerencia' Parameter from Request
        # request.args.get() safely returns None if the parameter is missing.
        gerencia_param = request.args.get('gerencia')

        # 2. Determine Dynamic Date Ranges (same logic as before)
        today = date.today()
        current_year = today.year
        
        cy_start_date = date(current_year, 1, 1)
        cy_end_date = today
        py_start_date = date(current_year - 1, 1, 1)
        py_end_date = today.replace(year=today.year - 1)

        # 3. Define the Parameterized SQL Query with the conditional filter
        # The line `AND (:gerencia IS NULL OR Gerencia = :gerencia)` is the key.
        sql_query = text("""
            SELECT
                FechaOperacion,
                MAX(Demanda) AS MaxDemanda
            FROM (
                SELECT
                    FechaOperacion,
                    HoraOperacion,
                    SUM(Demanda) AS Demanda
                FROM Demanda
                WHERE
                    Sistema = 'SIN'
                    -- This condition handles the optional 'gerencia' parameter
                    AND (:gerencia IS NULL OR Gerencia = :gerencia)
                    AND (
                        -- Range for the current year-to-date
                        (FechaOperacion BETWEEN :cy_start AND :cy_end)
                        OR
                        -- Range for the same period in the previous year
                        (FechaOperacion BETWEEN :py_start AND :py_end)
                    )
                GROUP BY
                    FechaOperacion,
                    HoraOperacion
            ) AS Subconsulta
            GROUP BY
                FechaOperacion
            ORDER BY
                FechaOperacion;
        """)

        # 4. Bind parameters and Execute the Query
        # We bind `gerencia_param` directly. If it's None, the SQL condition works as intended.
        bound_query = sql_query.bindparams(
            cy_start=cy_start_date,
            cy_end=cy_end_date,
            py_start=py_start_date,
            py_end=py_end_date,
            gerencia=gerencia_param 
        )
        
        results = db.session.execute(bound_query).fetchall()

        # 5. Process and Structure the Data
        current_year_data = []
        previous_year_data = []

        for row in results:
            record = {
                "Fecha": row.FechaOperacion.isoformat(),
                # Convert the result to a float for JSON
                "MaxDemanda_MWh": float(row.MaxDemanda) if row.MaxDemanda is not None else None
            }
            
            if row.FechaOperacion.year == current_year:
                current_year_data.append(record)
            else:
                previous_year_data.append(record)

        # 6. Return the structured JSON response
        return jsonify({
            "status": "success",
            "filter": {
                "gerencia": gerencia_param if gerencia_param else "ALL"
            },
            "dateRanges": {
                "currentYear": {"start": cy_start_date.isoformat(), "end": cy_end_date.isoformat()},
                "previousYear": {"start": py_start_date.isoformat(), "end": py_end_date.isoformat()}
            },
            "currentYearData": current_year_data,
            "previousYearData": previous_year_data
        })

    except Exception as e:
        # Basic error handling
        return jsonify({"status": "error", "message": str(e)}), 500

def get_demanda_aggregates_for_comparison_dates():
    """
    Calculates aggregated Demand data (average, max, min) for the latest date
    available in the Demanda table and the date one week prior for Sistema='SIN'.
    """
    try:
        # 1. Determine the most recent date from the Demanda table
        # This is more robust than using GETDATE() as data may not be current.
        latest_date_obj = db.session.query(db.func.max(DemandRecord.FechaOperacion)).scalar()

        if not latest_date_obj:
            # Return an empty response if the table has no data
            return jsonify({
                "message": "No data found in Demanda table.",
                "latest_day_records": [],
                "previous_week_day_records": []
            })

        # 2. Determine the date 7 days prior
        previous_week_date_obj = latest_date_obj - timedelta(days=7)

        # 3. Check if data actually exists for the prior date to avoid querying for nothing
        previous_week_date_exists = db.session.query(DemandRecord.FechaOperacion)\
            .filter(DemandRecord.FechaOperacion == previous_week_date_obj).limit(1).scalar()

        dates_to_query = [latest_date_obj]
        if previous_week_date_exists:
            dates_to_query.append(previous_week_date_obj)
        else:
            # Optionally log a warning if you have a logger configured
            print(f"Warning: No demand data found for {previous_week_date_obj.isoformat()}")


        # 4. Define the parameterized SQL query with aliases for clarity
        sql_query = text("""
            SELECT
                Gerencia,
                FechaOperacion,
                AVG(Demanda) AS Promedio_Demanda,
                MAX(Demanda) AS Maximo_Demanda,
                MIN(Demanda) AS Minimo_Demanda
            FROM
                Demanda
            WHERE
                FechaOperacion IN :dates_to_query
                AND Sistema = 'SIN'
            GROUP BY
                Gerencia,
                FechaOperacion
            ORDER BY
                Gerencia,
                FechaOperacion;
        """)

        # 5. Bind the list of dates to the IN clause
        bound_query = sql_query.bindparams(
            bindparam('dates_to_query', value=dates_to_query, expanding=True)
        )

        results = db.session.execute(bound_query).fetchall()

        # 6. Process and structure the results
        latest_day_records = []
        previous_week_day_records = []

        for row in results:
            record = {
                "Gerencia": row.Gerencia,
                "Fecha": row.FechaOperacion.isoformat(),
                "Promedio_Demanda": float(row.Promedio_Demanda) if row.Promedio_Demanda is not None else None,
                "Maximo_Demanda": float(row.Maximo_Demanda) if row.Maximo_Demanda is not None else None,
                "Minimo_Demanda": float(row.Minimo_Demanda) if row.Minimo_Demanda is not None else None,
            }
            if row.FechaOperacion == latest_date_obj:
                latest_day_records.append(record)
            elif row.FechaOperacion == previous_week_date_obj:
                previous_week_day_records.append(record)

        # 7. Return the final structured data
        return jsonify({
            "latest_date": latest_date_obj.isoformat() if latest_date_obj else None,
            "previous_week_date": previous_week_date_obj.isoformat() if previous_week_date_exists else None,
            "latest_day_records": latest_day_records,
            "previous_week_day_records": previous_week_day_records,
        })

    except Exception as e:
        # Proper error handling
        print(f"An error occurred: {e}") # Or use a real logger
        return jsonify({"status": "error", "message": "An internal error occurred."}), 500