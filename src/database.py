import sqlalchemy as sa
from src.config import DB_URL_OMS, DB_URL_TABLET, DB_URL_DW, DB_URL_GEOCOM

def get_connection(tipo):
    
    """ Carga un DataFrame en la base de datos """
    try:
       

        # Crear la conexión con la BD
        if tipo == 'oms':
            engine = sa.create_engine(DB_URL_OMS,fast_executemany=True)
        
        if tipo == 'tablet':
            engine = sa.create_engine(DB_URL_TABLET,fast_executemany=True)

        if tipo == 'dw':
            engine = sa.create_engine(DB_URL_DW,fast_executemany=True)
            
        if tipo == 'geocom':
            engine = sa.create_engine(DB_URL_GEOCOM,fast_executemany=True)

        return engine

    except Exception as e:
        print(f"Error al crear la conexion {tipo}: {e}")
