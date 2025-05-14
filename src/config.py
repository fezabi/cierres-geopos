# Configuración de la API y la base de datos

import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de la API
API_URL = os.getenv("API_URL")
API_USER = os.getenv("API_USER")
API_PASSWORD = os.getenv("API_PASSWORD")

# Configuración de la base de datos
DB_USER_OMS = os.getenv("DB_USER_OMS")
DB_PASSWORD_OMS = os.getenv("DB_PASSWORD_OMS")
DB_HOST_OMS = os.getenv("DB_HOST_OMS")
DB_PORT_OMS = os.getenv("DB_PORT_OMS")
DB_NAME_OMS = os.getenv("DB_NAME_OMS")

DB_USER_TABLET = os.getenv("DB_USER_TABLET")
DB_PASSWORD_TABLET = os.getenv("DB_PASSWORD_TABLET")
DB_HOST_TABLET = os.getenv("DB_HOST_TABLET")
DB_PORT_TABLET = os.getenv("DB_PORT_TABLET")
DB_NAME_TABLET = os.getenv("DB_NAME_TABLET")

DB_USER_DW = os.getenv("DB_USER_DW")
DB_PASSWORD_DW = os.getenv("DB_PASSWORD_DW")
DB_HOST_DW = os.getenv("DB_HOST_DW")
DB_PORT_DW = os.getenv("DB_PORT_DW")
DB_NAME_DW = os.getenv("DB_NAME_DW")

DB_USER_GEOCOM = os.getenv("DB_USER_GEOCOM")
DB_PASSWORD_GEOCOM = os.getenv("DB_PASSWORD_GEOCOM")
DB_HOST_GEOCOM = os.getenv("DB_HOST_GEOCOM")
DB_PORT_GEOCOM = os.getenv("DB_PORT_GEOCOM")
DB_NAME_GEOCOM = os.getenv("DB_NAME_GEOCOM")

DB_URL_OMS = f"mysql+pymysql://{DB_USER_OMS}:{DB_PASSWORD_OMS}@{DB_HOST_OMS}:{DB_PORT_OMS}/{DB_NAME_OMS}"
DB_URL_TABLET = f"mysql+pymysql://{DB_USER_TABLET}:{DB_PASSWORD_TABLET}@{DB_HOST_TABLET}:{DB_PORT_TABLET}/{DB_NAME_TABLET}"
DB_URL_DW = f"mssql+pyodbc://{DB_USER_DW}:{DB_PASSWORD_DW}@{DB_HOST_DW}:{DB_PORT_DW}/{DB_NAME_DW}?driver=ODBC+Driver+17+for+SQL+Server"
DB_URL_GEOCOM = f"mssql+pyodbc://{DB_USER_GEOCOM}:{DB_PASSWORD_GEOCOM}@{DB_HOST_GEOCOM}:{DB_PORT_GEOCOM}/{DB_NAME_GEOCOM}?driver=ODBC+Driver+17+for+SQL+Server"