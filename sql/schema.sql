-- =============================================================
-- Esquema de la base analítica (SQLite)
-- Patrón: tabla de hechos limpia + tabla agregada (resumen diario)
-- IF NOT EXISTS hace que el script pueda ejecutarse N veces sin error.
-- =============================================================

-- Tabla de hechos: una fila por venta, ya limpia.
-- id_venta es PRIMARY KEY => la base impide duplicados por sí misma.
CREATE TABLE IF NOT EXISTS ventas (
    id_venta        INTEGER PRIMARY KEY,
    fecha           TEXT    NOT NULL,              -- formato ISO YYYY-MM-DD
    cliente         TEXT    NOT NULL,
    producto        TEXT    NOT NULL,
    categoria       TEXT    NOT NULL,
    cantidad        INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario REAL    NOT NULL CHECK (precio_unitario >= 0),
    importe_total   REAL    NOT NULL,              -- cantidad * precio_unitario
    sucursal        TEXT    NOT NULL,
    fecha_carga     TEXT    NOT NULL DEFAULT (datetime('now'))
);

-- Tabla agregada: total vendido por día, sucursal y categoría.
-- La clave compuesta evita filas repetidas para la misma combinación.
CREATE TABLE IF NOT EXISTS resumen_ventas_diario (
    fecha            TEXT    NOT NULL,
    sucursal         TEXT    NOT NULL,
    categoria        TEXT    NOT NULL,
    cantidad_ventas  INTEGER NOT NULL,
    unidades         INTEGER NOT NULL,
    importe_total    REAL    NOT NULL,
    PRIMARY KEY (fecha, sucursal, categoria)
);

-- Índice para consultas analíticas frecuentes
CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON ventas (fecha);
