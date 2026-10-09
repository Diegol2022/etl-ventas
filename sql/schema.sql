-- =============================================================
-- Esquema de la base analítica (DDL de referencia, SQLite)
-- Origen: dataset "E-Commerce Data" de Kaggle (carrie1/ecommerce-data)
-- Patrón: tabla de hechos limpia + tabla agregada (resumen diario)
--
-- El script crea estas tablas desde los modelos de SQLAlchemy
-- (models.py) con Base.metadata.create_all(), que es equivalente.
-- Este archivo documenta el DDL y permite crear el esquema a mano:
--     sqlite3 ecommerce.db < sql/schema.sql
-- IF NOT EXISTS hace que pueda ejecutarse N veces sin error.
-- =============================================================

-- Tabla de hechos: una fila por producto dentro de cada factura.
-- La clave primaria compuesta (factura, codigo_producto) impide duplicados.
CREATE TABLE IF NOT EXISTS ventas (
    factura          VARCHAR(10)  NOT NULL,              -- InvoiceNo
    codigo_producto  VARCHAR(20)  NOT NULL,              -- StockCode
    descripcion      VARCHAR(255) NOT NULL,              -- Description
    cantidad         INTEGER      NOT NULL,              -- Quantity (consolidada)
    precio_unitario  FLOAT        NOT NULL,              -- UnitPrice
    importe_total    FLOAT        NOT NULL,              -- cantidad * precio_unitario
    fecha_factura    DATETIME     NOT NULL,              -- InvoiceDate
    id_cliente       INTEGER      NOT NULL,              -- CustomerID
    pais             VARCHAR(50)  NOT NULL,              -- Country
    fecha_carga      DATETIME     NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (factura, codigo_producto),
    CONSTRAINT ck_ventas_cantidad_positiva CHECK (cantidad > 0),
    CONSTRAINT ck_ventas_precio_positivo   CHECK (precio_unitario > 0)
);

-- Tabla agregada: totales por día y país.
CREATE TABLE IF NOT EXISTS resumen_ventas_diario (
    fecha              DATE        NOT NULL,
    pais               VARCHAR(50) NOT NULL,
    cantidad_facturas  INTEGER     NOT NULL,
    unidades           INTEGER     NOT NULL,
    importe_total      FLOAT       NOT NULL,
    PRIMARY KEY (fecha, pais)
);

-- Índices para consultas analíticas frecuentes
CREATE INDEX IF NOT EXISTS idx_ventas_fecha_factura ON ventas (fecha_factura);
CREATE INDEX IF NOT EXISTS idx_ventas_pais ON ventas (pais);
