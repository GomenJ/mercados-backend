from flask import current_app
import os
from app import create_app, db
from dotenv import load_dotenv

# Get configuration name from environment or use default
load_dotenv()
config_name = os.getenv('FLASK_ENV', 'default')
app = create_app(config_name)

@app.cli.command("init-db")
def init_db_command():
    """Initializes the database by creating all tables."""
    # Make sure this is called within an application context
    with app.app_context():
        db.create_all()
    current_app.logger.info("Database initialized.")

if __name__ == '__main__':
    # Use host='0.0.0.0' to be accessible externally (e.g., in Docker)
    # Port can be configured via environment variable if needed
    port = int(os.environ.get("PORT", 5001))
    # Debug mode should be controlled by FLASK_ENV or config, not hardcoded here
    app.run(host='0.0.0.0', port=port, debug=app.config['DEBUG'])

# mercados-backend/
# ├── run.py
# ├── config.py
# ├── app/
# |   ├── __init__.py
# |   ├── api/
# |   |   ├── health_check.py
# |   |   └── v1/
# |   |       ├── demand.py
# |   |       ├── generic_pml_pnd.py
# |   |       └── capacidad_transferencia.py
# |   └── models/
# |       ├── capacidad_transferencia_record.py
# |       ├── demand_record.py
# |       └── pml_pnd_records.py
# ├── .env
# |── .gitignore
# |── requirements.txt 
# |── mercado.log 
# └── .env.example