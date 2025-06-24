from flask import  request, jsonify, current_app

from .. import db 
from sqlalchemy import text

def get_last_mediciones_per_day():
    """
    Get a list of all mediciones.
    This endpoint retrieves all mediciones from the database.
    
    URL Examples:
    - /api/mediciones (for all gerencias)
    - /api/mediciones?days=7 (filtered for last 7 days)
    """
    try:
        # 1. Get Optional 'days' Parameter from Request
        # request.args.get() safely returns None if the parameter is missing.
        days_param = request.args.get('days', default=7, type=int)

        # 3. Define the Parameterized SQL Query with the conditional filter
        sql_query = text("""
                        SELECT
                            FechaMedicion,
                            Pseudonimo,
                            SUM(kWhER_Avg) / 1000 AS TotalDaily_MWhER
                        FROM
                            (
                                SELECT
                                    AVG(kWhER) AS kWhER_Avg,
                                    Pseudonimo,
                                    FechaMedicion
                                FROM
                                    [Replica_Simex].[dbo].[Mediciones]
                                WHERE
                                    TipoMedidor = 'Principal'
                                    AND FechaMedicion >= DATEADD(day, -:days, CAST(GETDATE() AS DATE))
                                GROUP BY
                                    Hora,
                                    FechaMedicion,
                                    Pseudonimo
                            ) AS HourlyAverages
                        GROUP BY
                            FechaMedicion,
                            Pseudonimo
                        ORDER BY
                            FechaMedicion DESC,
                            Pseudonimo;
        """)

        # 4. Bind parameters and Execute the Query
        # We bind `days_param` directly. If it's None, the SQL condition works as intended.
        bound_query = sql_query.bindparams(
            days=days_param
        )

        results = db.session.execute(bound_query).fetchall()
        # Serialize results to JSON-friendly format
        serialized_results = [
            {
                "FechaMedicion": row[0].isoformat(),
                "Pseudonimo": row[1],
                "TotalDaily_MWhER": float(row[2]) if row[2] is not None else None
            }
            for row in results
        ]
        current_app.logger.info(f"Fetched {len(results)} mediciones for the last {days_param} days.")

        # 6. Return the structured JSON response
        return jsonify({
            "status": "success",
            "data": serialized_results
        })

    except Exception as e:
        # Basic error handling
        return jsonify({"status": "error", "message": str(e)}), 500