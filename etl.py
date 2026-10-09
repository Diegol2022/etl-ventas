"""
ETL de ventas e-commerce: CSV de Kaggle -> base analítica (SQLAlchemy 2.0).

Origen: dataset "E-Commerce Data" (https://www.kaggle.com/datasets/carrie1/ecommerce-data)

Uso:
    python main.py
    python main.py --origen data/data.csv --destino ecommerce.db
    python main.py --db-url "mysql+pymysql://usuario:clave@localhost/ecommerce"
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, create_engine, delete, func, insert, select, text
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from models import Base, ResumenVentaDiario, Venta

BASE_DIR = Path(__file__).resolve().parent
ORIGEN_KAGGLE = BASE_DIR / "data" / "data.csv"               # dataset completo (no se versiona)
ORIGEN_MUESTRA = BASE_DIR / "data" / "muestra_ecommerce.csv"  # muestra incluida en el repo
DESTINO_DEFAULT = BASE_DIR / "ecommerce.db"

ENCODING_ORIGEN = "ISO-8859-1"        # el CSV de Kaggle no viene en UTF-8
FORMATO_FECHA = "%m/%d/%Y %H:%M"      # ej.: 12/1/2010 8:26
TAMANIO_LOTE = 5_000                  # filas por executemany durante la carga

# Columnas del CSV original -> nombres en la base
COLUMNAS_ORIGEN = {
    "InvoiceNo": "factura",
    "StockCode": "codigo_producto",
    "Description": "descripcion",
    "Quantity": "cantidad",
    "InvoiceDate": "fecha_factura",
    "UnitPrice": "precio_unitario",
    "CustomerID": "id_cliente",
    "Country": "pais",
}

# Códigos que no son productos (envíos, ajustes, comisiones, etc.)
CODIGOS_NO_PRODUCTO = {
    "POST", "DOT", "M", "D", "S", "B", "C2", "PADS", "CRUK",
    "BANK CHARGES", "AMAZONFEE",
}

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
        df = pd.read_csv(ruta, dtype=str, encoding=ENCODING_ORIGEN)
    except pd.errors.EmptyDataError:
        raise ETLError(f"El archivo de origen está vacío: {ruta}")
    except (pd.errors.ParserError, UnicodeDecodeError) as e:
        raise ETLError(f"No se pudo leer el CSV ({e})")

    faltantes = set(COLUMNAS_ORIGEN) - set(df.columns)
    if faltantes:
        raise ETLError(f"Faltan columnas en el origen: {sorted(faltantes)}")

    log.info("Filas extraídas: %d", len(df))
    return df[list(COLUMNAS_ORIGEN)].rename(columns=COLUMNAS_ORIGEN)


# ---------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------
def transformar(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Limpieza:
      1. recorta espacios y normaliza mayúsculas en textos
      2. elimina duplicados exactos (el dataset trae miles)
      3. descarta facturas canceladas (InvoiceNo que empieza con "C")
      4. descarta códigos que no son productos (POSTAGE, descuentos, etc.)
      5. convierte tipos y descarta nulos, fechas inválidas,
         cantidades <= 0, precios <= 0 y ventas sin cliente
      6. consolida el mismo producto repetido dentro de una factura
         (suma cantidades), lo que define la clave (factura, codigo_producto)
    Agregación:
      - resumen diario por fecha y país
    """
    inicial = len(df)
    df = df.copy()
    descartes: dict[str, int] = {}

    def descartar(motivo: str, mascara: pd.Series) -> None:
        nonlocal df
        descartes[motivo] = int(mascara.sum())
        df = df[~mascara]

    # 1. Normalización de textos
    for col in ["factura", "codigo_producto", "descripcion", "pais", "id_cliente"]:
        df[col] = df[col].str.strip()
    df["codigo_producto"] = df["codigo_producto"].str.upper()
    df["descripcion"] = df["descripcion"].str.upper().str.replace(r"\s+", " ", regex=True)
    df = df.replace("", pd.NA)

    # 2. Duplicados exactos
    descartar("duplicados exactos", df.duplicated())

    # 3. Cancelaciones
    descartar("facturas canceladas", df["factura"].str.startswith("C", na=False))

    # 4. Códigos que no son productos
    descartar("códigos no producto", df["codigo_producto"].isin(CODIGOS_NO_PRODUCTO))

    # 5. Tipos (lo inválido pasa a nulo) y validaciones
    df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")
    df["precio_unitario"] = pd.to_numeric(df["precio_unitario"], errors="coerce")
    df["id_cliente"] = pd.to_numeric(df["id_cliente"], errors="coerce")
    df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], format=FORMATO_FECHA,
                                         errors="coerce")
    descartar("sin cliente", df["id_cliente"].isna())
    descartar("nulos o fechas inválidas", df.isna().any(axis=1))
    descartar("cantidad o precio <= 0",
              (df["cantidad"] <= 0) | (df["precio_unitario"] <= 0))

    # 6. Consolidación por (factura, codigo_producto)
    df["importe_total"] = df["cantidad"] * df["precio_unitario"]
    antes = len(df)
    ventas = (
        df.groupby(["factura", "codigo_producto"], as_index=False, sort=False)
          .agg(descripcion=("descripcion", "first"),
               cantidad=("cantidad", "sum"),
               importe_total=("importe_total", "sum"),
               fecha_factura=("fecha_factura", "first"),
               id_cliente=("id_cliente", "first"),
               pais=("pais", "first"))
    )
    descartes["líneas consolidadas"] = antes - len(ventas)
    ventas["precio_unitario"] = (ventas["importe_total"] / ventas["cantidad"]).round(4)
    ventas["importe_total"] = ventas["importe_total"].round(2)
    ventas = ventas.astype({"cantidad": int, "id_cliente": int})

    log.info("Filas válidas tras limpieza: %d de %d", len(ventas), inicial)
    for motivo, n in descartes.items():
        if n:
            log.info("  - %s: %d", motivo, n)

    # Agregación
    resumen = (
        ventas.assign(fecha=ventas["fecha_factura"].dt.date)
              .groupby(["fecha", "pais"], as_index=False)
              .agg(cantidad_facturas=("factura", "nunique"),
                   unidades=("cantidad", "sum"),
                   importe_total=("importe_total", "sum"))
    )
    log.info("Filas del resumen diario: %d", len(resumen))
    return ventas, resumen


# ---------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------
COLUMNAS_VENTA = [
    "factura", "codigo_producto", "descripcion", "cantidad", "precio_unitario",
    "importe_total", "fecha_factura", "id_cliente", "pais",
]
CLAVE_VENTA = ["factura", "codigo_producto"]


def conectar(db_url: str) -> Engine:
    """
    Crea el Engine de SQLAlchemy y prueba la conexión.
    create_engine() es "perezoso": no conecta hasta el primer uso,
    por eso se ejecuta un SELECT 1 para detectar errores temprano.
    """
    try:
        engine = create_engine(db_url, pool_pre_ping=True)
    except (SQLAlchemyError, ImportError) as e:  # URL inválida o driver faltante
        raise ETLError(f"URL de base de datos inválida o driver no instalado: {e}")

    if engine.dialect.name == "sqlite" and engine.url.database:
        Path(engine.url.database).parent.mkdir(parents=True, exist_ok=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except OperationalError as e:
        url_segura = engine.url.render_as_string(hide_password=True)
        raise ETLError(f"No se pudo conectar a la base ({url_segura}): {e.orig}")
    return engine


def crear_esquema(engine: Engine) -> None:
    """Crea las tablas de models.py solo si no existen (equivale a IF NOT EXISTS)."""
    try:
        Base.metadata.create_all(engine)
    except SQLAlchemyError as e:
        raise ETLError(f"Error al crear el esquema: {e}")


def _sentencia_upsert(dialecto: str):
    """
    INSERT que, si (factura, codigo_producto) ya existe, actualiza la fila
    en lugar de duplicarla. SQLAlchemy genera la sintaxis de cada motor.
    """
    actualizables = [c for c in COLUMNAS_VENTA if c not in CLAVE_VENTA]
    if dialecto == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as dialect_insert
        stmt = dialect_insert(Venta.__table__)
        return stmt.on_conflict_do_update(
            index_elements=CLAVE_VENTA,
            set_={**{c: stmt.excluded[c] for c in actualizables},
                  "fecha_carga": func.now()},
        )
    if dialecto in ("mysql", "mariadb"):
        from sqlalchemy.dialects.mysql import insert as dialect_insert
        stmt = dialect_insert(Venta.__table__)
        return stmt.on_duplicate_key_update(
            {**{c: stmt.inserted[c] for c in actualizables},
             "fecha_carga": func.now()},
        )
    raise ETLError(f"Motor no soportado para UPSERT: {dialecto}")


def _a_registros(ventas: pd.DataFrame) -> list[dict]:
    """Convierte el DataFrame a dicts con tipos nativos de Python."""
    return [
        {
            "factura": str(f.factura),
            "codigo_producto": str(f.codigo_producto),
            "descripcion": str(f.descripcion),
            "cantidad": int(f.cantidad),
            "precio_unitario": float(f.precio_unitario),
            "importe_total": float(f.importe_total),
            "fecha_factura": f.fecha_factura.to_pydatetime(),
            "id_cliente": int(f.id_cliente),
            "pais": str(f.pais),
        }
        for f in ventas.itertuples(index=False)
    ]


def cargar(engine: Engine, ventas: pd.DataFrame) -> None:
    """
    Carga idempotente (se puede ejecutar varias veces sin duplicar):
      - ventas: UPSERT por (factura, codigo_producto), en lotes
      - resumen: limpieza previa (DELETE) y recálculo desde la tabla de
        hechos con INSERT ... SELECT, así refleja también cargas anteriores.
    Todo corre en una única transacción (session.begin()): si algo falla,
    se hace rollback y la base queda como estaba.
    """
    registros = _a_registros(ventas)
    upsert = _sentencia_upsert(engine.dialect.name)
    contar_ventas = select(func.count()).select_from(Venta)

    dia = func.date(Venta.fecha_factura)
    agregado = (
        select(
            dia, Venta.pais,
            func.count(Venta.factura.distinct()),
            func.sum(Venta.cantidad),
            func.sum(Venta.importe_total),
        )
        .group_by(dia, Venta.pais)
    )
    insertar_resumen = insert(ResumenVentaDiario.__table__).from_select(
        ["fecha", "pais", "cantidad_facturas", "unidades", "importe_total"],
        agregado,
    )

    try:
        with Session(engine) as session, session.begin():
            antes = session.scalar(contar_ventas)
            for i in range(0, len(registros), TAMANIO_LOTE):
                session.execute(upsert, registros[i:i + TAMANIO_LOTE])
            despues = session.scalar(contar_ventas)

            session.execute(delete(ResumenVentaDiario.__table__))
            session.execute(insertar_resumen)
            filas_resumen = session.scalar(
                select(func.count()).select_from(ResumenVentaDiario))
    except SQLAlchemyError as e:
        raise ETLError(f"Error durante la carga (se hizo rollback): {e}")

    nuevas = despues - antes
    log.info("Ventas: %d nuevas, %d actualizadas, %d en total",
             nuevas, len(registros) - nuevas, despues)
    log.info("Resumen diario recalculado: %d filas", filas_resumen)


# ---------------------------------------------------------------
# ORQUESTACIÓN
# ---------------------------------------------------------------
def resolver_origen(origen: Path | None) -> Path:
    """
    Si no se indica --origen, usa el dataset completo de Kaggle (data/data.csv);
    si no fue descargado, usa la muestra incluida en el repositorio.
    """
    if origen is not None:
        return origen
    if ORIGEN_KAGGLE.exists():
        return ORIGEN_KAGGLE
    log.warning("No se encontró %s (ver README para descargarlo). "
                "Se usa la muestra %s", ORIGEN_KAGGLE.name, ORIGEN_MUESTRA.name)
    return ORIGEN_MUESTRA


def main() -> int:
    parser = argparse.ArgumentParser(description="ETL de ventas e-commerce -> base analítica")
    parser.add_argument("--origen", type=Path, default=None,
                        help="CSV de origen (default: data/data.csv o, si no existe, "
                             "data/muestra_ecommerce.csv)")
    parser.add_argument("--destino", type=Path, default=DESTINO_DEFAULT,
                        help="Archivo SQLite de destino (default: ecommerce.db)")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL"),
                        help="URL SQLAlchemy de destino; tiene prioridad sobre "
                             "--destino. También se puede pasar en DATABASE_URL.")
    args = parser.parse_args()
    db_url = args.db_url or f"sqlite:///{args.destino}"

    engine = None
    try:
        crudos = extraer(resolver_origen(args.origen))
        ventas, _ = transformar(crudos)
        if ventas.empty:
            log.warning("No hay filas válidas para cargar.")
            return 0
        engine = conectar(db_url)
        crear_esquema(engine)
        cargar(engine, ventas)
        log.info("ETL finalizado OK -> %s",
                 engine.url.render_as_string(hide_password=True))
        return 0
    except ETLError as e:
        log.error(e)
        return 1
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
