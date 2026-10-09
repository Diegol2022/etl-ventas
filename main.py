"""
Punto de entrada del proyecto.

Ejecuta el flujo ETL completo: Extract -> Transform -> Load.

Uso:
    python main.py
    python main.py --origen data/ventas_raw.csv --destino ventas.db
"""

import sys

from etl import main

if __name__ == "__main__":
    sys.exit(main())
