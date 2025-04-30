import os
from app import create_app
from dotenv import load_dotenv

# Get configuration name from environment or use default
load_dotenv()
config_name = os.getenv('FLASK_ENV', 'default')
app = create_app(config_name)

if __name__ == '__main__':
    # Use host='0.0.0.0' to be accessible externally (e.g., in Docker)
    # Port can be configured via environment variable if needed
    port = int(os.environ.get("PORT", 5001))
    # Debug mode should be controlled by FLASK_ENV or config, not hardcoded here
    app.run(host='0.0.0.0', port=port)