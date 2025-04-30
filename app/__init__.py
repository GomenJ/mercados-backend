import os
import logging
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_restful import Api
from flask_cors import CORS
# from flask_migrate import Migrate # Uncomment if using migrations

# Import configurations and error handlers
from .config import config_by_name
from .errors import error_handlers

# Instantiate extensions (without app object initially)
db = SQLAlchemy()
ma = Marshmallow()
cors = CORS()
# migrate = Migrate() # Uncomment if using migrations

# Import models here AFTER db is instantiated, so they correctly inherit from db.Model
# This also helps Flask-Migrate detect models.
from . import models # noqa


def create_app(config_name=None):
    """Application Factory Function"""
    if config_name is None:
        config_name = os.getenv('FLASK_ENV', 'default')

    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    # --- Initialize Extensions with App ---
    db.init_app(app)
    ma.init_app(app)
    cors.init_app(app) # Apply CORS globally or configure specific resources
    # migrate.init_app(app, db) # Uncomment if using migrations

    # Initialize Flask-RESTful AFTER app creation and config loading
    # Pass custom error handling to Api constructor if desired
    api = Api(app)

    # --- Setup Logging ---
    log_level = app.config.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        filename="mercado.log", # Consider rotating file handlers for production
        level=log_level,
        format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s' # Example format
    )
    app.logger.info(f"App created with config: {config_name}")
    app.logger.info(f"Database URI: {app.config.get('SQLALCHEMY_DATABASE_URI')}") # Be careful logging URIs with passwords

    # --- Register Error Handlers ---
    for exc_or_code, handler in error_handlers.items():
        app.register_error_handler(exc_or_code, handler)

    # --- Import and Register Routes/Resources ---
    from .resources.routes import initialize_routes
    initialize_routes(api) # Pass the api instance to the route initializer

    # --- Database Creation (Run once or use migrations) ---
    with app.app_context():
        app.logger.info("Checking/Creating database tables if they don't exist...")
        try:
             # Use db.create_all() for initial setup or simple cases.
             # For production and schema changes, Flask-Migrate is recommended.
             db.create_all()
             app.logger.info("Database tables checked/created successfully.")
        except Exception as e:
            app.logger.exception(f"CRITICAL ERROR: Failed to create/check database tables: {e}")



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