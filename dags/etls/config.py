# config.py
import os
from dotenv import load_dotenv

# Load variables de entorno desde el archivo .env
load_dotenv()

# Credenciales para ambas bases de datos
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME_SOURCE = "CrashTraffic"
DB_NAME_DIM = "CrashTraffic_Dimensional"

# Crear URLs de conexión para SQLAlchemy
SOURCE_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_SOURCE}"
DIM_DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_DIM}"