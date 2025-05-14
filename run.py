# run.py
import sys
import os
import argparse
from src.main import procesar_cierres

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

def parse_arguments():
    parser = argparse.ArgumentParser(description='Ejecutar ETL con destino y tipo de ejecución.')
    parser.add_argument('--modulos', nargs='+', required=True, help='Lista de módulos a ejecutar')
    parser.add_argument('--localid', required=True, help='Número de local')
    parser.add_argument('--pos', required=True, help='Número de la caja (1/2/3 o None)')
    parser.add_argument('--fecha_ini', required=True, help='Fecha cierre inicial YYYYMMDD')
    parser.add_argument('--fecha_fin', required=True, help='Fecha cierre final YYYYMMDD')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()

    config = {
        "modulos": args.modulos,
        "localid": None if args.localid == "None" else int(args.localid),
        "pos": None if args.pos == "None" else int(args.pos),
        "fecha_ini": args.fecha_ini,
        "fecha_fin": args.fecha_fin,
        "iva_rate": 0.19
    }

    procesar_cierres(config)
