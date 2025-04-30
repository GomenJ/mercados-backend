# Import resources
from .demand import DemandResource
from .generic_mda_mtr import GenericBatchResource, DataExistenceResource
from .health_check import HealthCheckResource
from .capacidad_transferencia import CapacidadTransferenciaResource

def initialize_routes(api):
    """Initializes all the API routes."""

    # --- Mercado Endpoints ---
    api.add_resource(DemandResource, '/api/v1/mercado/demanda', endpoint='demand_record')

    api.add_resource(GenericBatchResource, '/api/v1/mercado/mda_mtr/<string:data_type>', endpoint='generic_batch')

    api.add_resource(DataExistenceResource, '/api/v1/mercado/check/<string:data_type>/<string:fecha>', endpoint='data_existence')

    # Add other resources here
 # --- Add route for the new resource ---
    api.add_resource(CapacidadTransferenciaResource,
                     '/api/v1/mercado/capacidad-transferencia', # URL for creating/listing
                     endpoint='capacidad_transferencia_list')

    # Health check endpoint
    api.add_resource(HealthCheckResource, '/api/v1/health', endpoint='health_check')