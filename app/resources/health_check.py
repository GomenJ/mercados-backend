from flask import request, current_app
from flask_restful import Resource
from marshmallow import ValidationError
from sqlalchemy.exc import IntegrityError

# Import db, models, schemas

class HealthCheckResource(Resource):
    """Resource for health check."""

    def get(self):
        """
        Health check endpoint.
        Returns a simple JSON response to indicate the service is running.
        """
        return {"status": "success", "message": "Service is running"}, 200
