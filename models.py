"""
Capa de persistencia con SQLAlchemy 2.0 (estilo declarativo tipado).

Define las tablas de destino como modelos ORM. Equivale al DDL de
sql/schema.sql; Base.metadata.create_all() las crea solo si no existen
(checkfirst=True por defecto, igual que CREATE TABLE IF NOT EXISTS).
"""

from datetime import date, datetime

from sqlalchemy import CheckConstraint, Date, DateTime, Float, Index, Integer, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Clase base de todos los modelos."""


class Venta(Base):
    """
    Tabla de hechos: una fila por producto dentro de cada factura.
    Clave primaria natural compuesta: (factura, codigo_producto).
    """

    __tablename__ = "ventas"
    __table_args__ = (
        CheckConstraint("cantidad > 0", name="ck_ventas_cantidad_positiva"),
        CheckConstraint("precio_unitario > 0", name="ck_ventas_precio_positivo"),
        Index("idx_ventas_fecha_factura", "fecha_factura"),
        Index("idx_ventas_pais", "pais"),
    )

    factura: Mapped[str] = mapped_column(String(10), primary_key=True)
    codigo_producto: Mapped[str] = mapped_column(String(20), primary_key=True)
    descripcion: Mapped[str] = mapped_column(String(255))
    cantidad: Mapped[int] = mapped_column(Integer)
    precio_unitario: Mapped[float] = mapped_column(Float)
    importe_total: Mapped[float] = mapped_column(Float)
    fecha_factura: Mapped[datetime] = mapped_column(DateTime)
    id_cliente: Mapped[int] = mapped_column(Integer)
    pais: Mapped[str] = mapped_column(String(50))
    fecha_carga: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return (f"Venta(factura={self.factura}, producto={self.codigo_producto}, "
                f"importe={self.importe_total})")


class ResumenVentaDiario(Base):
    """Tabla agregada: totales por día y país."""

    __tablename__ = "resumen_ventas_diario"

    fecha: Mapped[date] = mapped_column(Date, primary_key=True)
    pais: Mapped[str] = mapped_column(String(50), primary_key=True)
    cantidad_facturas: Mapped[int] = mapped_column(Integer)
    unidades: Mapped[int] = mapped_column(Integer)
    importe_total: Mapped[float] = mapped_column(Float)
