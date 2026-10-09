"""
ETL de ventas: CSV transaccional -> SQLite analítica.

Uso:
    python etl.py
    python etl.py --origen data/ventas_raw.csv --destino ventas.db
"""

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
ORIGEN_DEFAULT = BASE_DIR / "data" / "ventas_raw.csv"
DESTINO_DEFAULT = BASE_DIR / "ventas.db"
SCHEMA_SQL = BASE_DIR / "sql" / "schema.sql"

COLUMNAS_ESPERADAS = [
    "id_venta", "fecha", "cliente", "producto",
    "categoria", "cantidad", "precio_unitario", "sucursal",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")


class ETLError(Exception):
    """Error controlado del proceso ETL."""


# ---------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------
def extraer(ruta: Path) -> pd.DataFrame:
    """Lee el CSV de origen y valida que tenga las columnas esperadas."""
    log.info("Extrayendo datos desde %s", ruta)
    if not ruta.exists():
        raise ETLError(f"No se encontró el archivo de origen: {ruta}")
    try:
        df = pd.read_csv(ruta, dtype=str, encoding="utf-8")
    except pd.errors.EmptyDataError:
        raise ETLError(f"El archivo de origen está vacío: {ruta}")
    except (pd.errors.ParserError, UnicodeDecodeError) as e:
        raise ETLError(f"No se pudo leer el CSV ({e})")

    faltantes = set(COLUMNAS_ESPERADAS) - set(df.columns)
    if faltantes:
        raise ETLError(f"Faltan columnas en el origen: {sorted(faltantes)}")

    log.info("Filas extraídas: %d", len(df))
    return df


# ---------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------
def transformar(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Limpieza:
      - recorta espacios y normaliza mayúsculas en textos
      - convierte tipos (fecha, enteros, decimales)
      - descarta filas con nulos, fechas inválidas o cantidades <= 0
      - elimina duplicados por id_venta
      - calcula importe_total
    Agregación:
      - resumen diario por fecha, sucursal y categoría
    """
    inicial = len(df)
    df = df.copy()

    # Normalización de textos
    for col in ["cliente", "producto", "categoria", "sucursal"]:
        df[col] = df[col].str.strip()
    df["cliente"] = df["cliente"].str.title()
    df["categoria"] = df["categoria"].str.capitalize()
    df["sucursal"] = df["sucursal"].str.title()
    df = df.replace("", pd.NA)

    # Conversión de tipos (lo inválido pasa a nulo)
    df["id_venta"] = pd.to_numeric(df["id_venta"], errors="coerce").astype("Int64")
    df["fecha"] = pd.to_datetime(df["fecha"], format="%Y-%m-%d", errors="coerce")
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce").astype("Int64")
    df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce")

    # Filtrado de registros inválidos
    df = df.dropna(subset=COLUMNAS_ESPERADAS)
    df = df[(df["cantidad"] > 0) & (df["precio_unitario"] >= 0)]

    # Duplicados en el origen
    df = df.drop_duplicates(subset="id_venta", keep="first")

    # Columnas derivadas
    df["importe_total"] = df["cantidad"] * df["precio_unitario"]
    df["fecha"] = df["fecha"].dt.strftime("%Y-%m-%d")
    df = df.astype({"id_venta": int, "cantidad": int})

    log.info("Filas válidas tras limpieza: %d (descartadas: %d)",
             len(df), inicial - len(df))

    # Agregación
    resumen = (
        df.groupby(["fecha", "sucursal", "categoria"], as_index=False)
          .agg(cantidad_ventas=("id_venta", "count"),
               unidades=("cantidad", "sum"),
               importe_total=("importe_total", "sum"))
    )
    log.info("Filas del resumen diario: %d", len(resumen))
    return df, resumen


# ---------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------
def conectar(ruta_db: Path) -> sqlite3.Connection:
    """Abre la conexión a SQLite manejando errores comunes."""
    try:
        ruta_db.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(ruta_db, timeout=10)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except (sqlite3.OperationalError, PermissionError) as e:
        raise ETLError(f"No se pudo conectar a la base {ruta_db}: {e}")


def crear_esquema(conn: sqlite3.Connection) -> None:
    """Ejecuta el DDL (CREATE TABLE IF NOT EXISTS ...)."""
    if not SCHEMA_SQL.exists():
        raise ETLError(f"No se encontró el DDL: {SCHEMA_SQL}")
    try:
        conn.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))
    except sqlite3.Error as e:
        raise ETLError(f"Error al crear el esquema: {e}")


def cargar(conn: sqlite3.Connection, ventas: pd.DataFrame,
           resumen: pd.DataFrame) -> None:
    """
    Carga idempotente (se puede ejecutar varias veces sin duplicar):
      - ventas: UPSERT por id_venta (INSERT ... ON CONFLICT DO UPDATE)
      - resumen: se recalcula desde la tabla de hechos completa
        (limpieza previa con DELETE + INSERT), así refleja también
        cargas anteriores.
    Todo corre dentro de una única transacción: si algo falla,
    se hace rollback y la base queda como estaba.
    """
    sql_upsert = """
        INSERT INTO ventas (id_venta, fecha, cliente, producto, categoria,
                            cantidad, precio_unitario, importe_total, sucursal)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id_venta) DO UPDATE SET
            fecha = excluded.fecha,
            cliente = excluded.cliente,
            producto = excluded.producto,
            categoria = excluded.categoria,
            cantidad = excluded.cantidad,
            precio_unitario = excluded.precio_unitario,
            importe_total = excluded.importe_total,
            sucursal = excluded.sucursal,
            fecha_carga = datetime('now')
    """
    filas = ventas[[
        "id_venta", "fecha", "cliente", "producto", "categoria",
        "cantidad", "precio_unitario", "importe_total", "sucursal",
    ]].itertuples(index=False, name=None)

    try:
        with conn:  # transacción: commit si todo OK, rollback si hay error
            antes = conn.execute("SELECT COUNT(*) FROM ventas").fetchone()[0]
            conn.executemany(sql_upsert, filas)
            despues = conn.execute("SELECT COUNT(*) FROM ventas").fetchone()[0]

            conn.execute("DELETE FROM resumen_ventas_diario")
            conn.execute("""
                INSERT INTO resumen_ventas_diario
                SELECT fecha, sucursal, categoria,
                       COUNT(*), SUM(cantidad), SUM(importe_total)
                FROM ventas
                GROUP BY fecha, sucursal, categoria
            """)
    except sqlite3.Error as e:
        raise ETLError(f"Error durante la carga (se hizo rollback): {e}")

    log.info("Ventas: %d nuevas, %d actualizadas, %d en total",
             despues - antes, len(ventas) - (despues - antes), despues)
    log.info("Resumen diario recalculado (%d filas en el lote actual)",
             len(resumen))


# ---------------------------------------------------------------
# ORQUESTACIÓN
# ---------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description="ETL de ventas CSV -> SQLite")
    parser.add_argument("--origen", type=Path, default=ORIGEN_DEFAULT,
                        help="CSV de origen (default: data/ventas_raw.csv)")
    parser.add_argument("--destino", type=Path, default=DESTINO_DEFAULT,
                        help="Base SQLite de destino (default: ventas.db)")
    args = parser.parse_args()

    conn = None
    try:
        crudos = extraer(args.origen)
        ventas, resumen = transformar(crudos)
        if ventas.empty:
            log.warning("No hay filas válidas para cargar.")
            return 0
        conn = conectar(args.destino)
        crear_esquema(conn)
        cargar(conn, ventas, resumen)
        log.info("ETL finalizado OK -> %s", args.destino)
        return 0
    except ETLError as e:
        log.error(e)
        return 1
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
