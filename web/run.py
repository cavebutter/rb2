"""Application Entry Point"""
import os
from app import create_app

# Detect environment from ENB variable
env = os.environ.get('FLASK_ENV', 'development')
app = create_app(env)

if __name__ == '__main__':
    app.run(
        host='0.0.0.0',
        port=5001,
        debug=(env == 'development'),
    )