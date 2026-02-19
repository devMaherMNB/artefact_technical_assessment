import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load env variables from root directory
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_db_engine():
    """
    Creates and returns a SQLAlchemy engine using environment variables.
    """
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    db = os.getenv('POSTGRES_DB')
    host = os.getenv('POSTGRES_HOST')
    port = os.getenv('POSTGRES_PORT')

    if not all([user, password, db, host]):
        print("ERROR: Missing database configuration in environment.")
        sys.exit(1)

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

# Singleton instance
engine = get_db_engine()
