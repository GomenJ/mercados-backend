# app/__init__.py
from flask import Flask, jsonify, send_from_directory, request
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
import logging
import os
from config import app_config # Import from config.py at the root
# from flask_migrate import Migrate # Uncomment if using migrations

# Import configurations and error handlers
db = SQLAlchemy()
cors = CORS()

def create_app(config_name=None):
    """Application Factory Function"""
    if config_name is None:
        config_name = os.getenv('FLASK_ENV', 'default')

    app = Flask(__name__, static_folder='static', static_url_path='') # Serve from app/static

    # Load configuration
    app.config.from_object(app_config[config_name])

    # Configure logging
    logging.basicConfig(filename="./mercado.log", filemode='a', 
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, app.config['LOGGING_LEVEL'], logging.INFO))
    if app.debug or app.testing:
        app.logger.setLevel(logging.DEBUG)
    else:
        app.logger.setLevel(logging.INFO)
    app.logger.info(f"Application starting with '{config_name}' configuration.")

    if not app.config.get('SQLALCHEMY_DATABASE_URI'):
        app.logger.error("FATAL ERROR: SQLALCHEMY_DATABASE_URI is not set. Application cannot start.")
        # In a real app, you might raise an exception or exit
        # For now, we'll let it potentially fail later during db init if not set.
        # raise RuntimeError("SQLALCHEMY_DATABASE_URI is not set.")

    # --- Initialize Extensions with App ---
    db.init_app(app) # Optional timeout for DB connections
    # cors.init_app(app, resources={r"/*": {"origins": "*"}}) # Configure CORS to allow all origins
    cors.init_app(app) # Apply CORS globally or configure specific resources
    # migrate.init_app(app, db) # Uncomment if using migrations

    # Initialize Flask-RESTful AFTER app creation and config loading
    # Pass custom error handling to Api constructor if desired

    # Register Blueprints
    from .api.health_check import health_check_bp
    from .api.v1.generic_mda_mtr import generic_mda_mtr_bp
    from .api.v1.capacidad_transferencia import capacidad_transferencia_bp
    from .api.v1.demand import demanda_bp
    from .api.v1.demanda_real_balance import demanda_real_balance_bp
    from .api.v1.mediciones import mediciones_bp

    app.register_blueprint(health_check_bp, url_prefix='/api/health_check')
    app.register_blueprint(generic_mda_mtr_bp, url_prefix='/api/v1/mda_mtr')
    app.register_blueprint(capacidad_transferencia_bp, url_prefix='/api/v1/capacidad_transferencia')
    app.register_blueprint(demanda_bp, url_prefix='/api/v1/demanda')
    app.register_blueprint(mediciones_bp, url_prefix='/api/v1/mediciones')
    app.register_blueprint(demanda_real_balance_bp, url_prefix='/api/v1/demanda_real_balance')

    # --- Basic Root Route (Optional - Can be removed if only API) ---
    @app.route("/")
    def index():
        """Basic index route showing API info"""
        return jsonify(
            {
                "message": "CENACE Data Storage API (Flask-RESTful)",
                "status": "running",
                "api_version": "v1",
                "timestamp": db.func.now().eval(session=db.session), # Use DB time if possible
            }
        )

    return app