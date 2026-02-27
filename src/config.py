import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

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

DB_USER_GEOCOM_QA = os.getenv("DB_USER_GEOCOM_QA")
DB_PASSWORD_GEOCOM_QA = os.getenv("DB_PASSWORD_GEOCOM_QA")
DB_HOST_GEOCOM_QA = os.getenv("DB_HOST_GEOCOM_QA")
DB_PORT_GEOCOM_QA = os.getenv("DB_PORT_GEOCOM_QA")
DB_NAME_GEOCOM_QA = os.getenv("DB_NAME_GEOCOM_QA")

DB_URL_DW = f"mssql+pyodbc://{DB_USER_DW}:{DB_PASSWORD_DW}@{DB_HOST_DW}:{DB_PORT_DW}/{DB_NAME_DW}?driver=ODBC+Driver+17+for+SQL+Server"
DB_URL_GEOCOM = f"mssql+pyodbc://{DB_USER_GEOCOM}:{DB_PASSWORD_GEOCOM}@{DB_HOST_GEOCOM}:{DB_PORT_GEOCOM}/{DB_NAME_GEOCOM}?driver=ODBC+Driver+17+for+SQL+Server"
DB_URL_GEOCOM_QA = f"mssql+pyodbc://{DB_USER_GEOCOM_QA}:{DB_PASSWORD_GEOCOM_QA}@{DB_HOST_GEOCOM_QA}:{DB_PORT_GEOCOM_QA}/{DB_NAME_GEOCOM_QA}?driver=ODBC+Driver+17+for+SQL+Server"