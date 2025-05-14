# run.py
import sys
import os
import argparse
from src.main import procesar_cierres

# Añadir 'src' al PYTHONPATH para que Python pueda encontrar los módulos en 'src'
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))     

def parse_arguments():
    parser = argparse.ArgumentParser(description='Ejecutar ETL con destino y tipo de ejecución.')
    parser.add_argument('--modulos', required=True, help='Módulo a ejecutar "VENTAS", "MEDIOS_PAGO", "REDONDEOS", "DEPOSITOS", "GUIAS"')
    parser.add_argument('--localid', required=True, help='Número de local')
    parser.add_argument('--pos', required=True, help='Número de la caja (1/2/3)')
    parser.add_argument('--fecha_ini', required=True, help='Fecha cierre inicial YYYYMMD')
    parser.add_argument('--fecha_fin', required=True, help='Fecha cierre final YYYYMMD')

    return parser.parse_args()

if __name__ == "__main__":
    # Parsear los argumentos de la línea de comandos
    args = parse_arguments()
    
    # "modulos": ["VENTAS", "MEDIOS_PAGO", "REDONDEOS", "DEPOSITOS", "GUIAS"],
    # "localid": '118,140,185', # Si quieres filtrar por LOCALID, asigna el número
    # "localid": 140, # Si quieres filtrar por LOCALID, asigna el número
    # "pos": 1,  # Si quieres filtrar por POS, asigna el número
    
    config = {
        "modulos": [args.modulos],
        "localid": args.localid,
        "pos": args.pos,
        "fecha_ini": args.par_ini,
        "fecha_fin": args.par_fin,
        "iva_rate": 0.19
    }

    procesar_cierres(config)
