# ETL de Ventas → SQLite

Flujo ETL (Extract, Transform, Load) automatizado en Python que toma ventas desde un origen transaccional (archivo CSV), las limpia y agrega, y las persiste en una base de datos analítica local (SQLite). El proceso es **repetible**: se puede ejecutar todas las veces que se quiera sin duplicar datos.

## Estructura del proyecto

```
etl-ventas/
├── data/
│   └── ventas_raw.csv      # Origen transaccional (con datos "sucios" a propósito)
├── sql/
│   └── schema.sql          # DDL de las tablas de destino
├── main.py                 # Punto de entrada: ejecuta el flujo completo
├── etl.py                  # Lógica ETL (extraer, transformar, cargar)
├── requirements.txt        # Dependencias de Python
├── .gitignore
└── README.md
```

## Requisitos

- Python 3.10 o superior
- pip
- Dependencias: `pandas` (ver `requirements.txt`). SQLite viene incluido en Python (`sqlite3`), no hace falta instalar un servidor de base de datos.

## Pasos para ejecutar

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/<tu-usuario>/etl-ventas.git
   cd etl-ventas
   ```
2. (Opcional, recomendado) Crear y activar un entorno virtual:
   ```bash
   python -m venv .venv
   source .venv/bin/activate        # Linux / macOS
   .venv\Scripts\activate           # Windows
   ```
3. Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```
4. Ejecutar el ETL:
   ```bash
   python main.py
   ```
   Se genera (o actualiza) el archivo `ventas.db` en la raíz del proyecto.

5. (Opcional) Usar otro origen o destino:
   ```bash
   python main.py --origen data/otro_archivo.csv --destino salida/analitica.db
   ```

Salida esperada:

```
[INFO] Extrayendo datos desde .../data/ventas_raw.csv
[INFO] Filas extraídas: 21
[INFO] Filas válidas tras limpieza: 16 (descartadas: 5)
[INFO] Filas del resumen diario: 16
[INFO] Ventas: 16 nuevas, 0 actualizadas, 16 en total
[INFO] Resumen diario recalculado (16 filas en el lote actual)
[INFO] ETL finalizado OK -> .../ventas.db
```

Si se vuelve a ejecutar, informa `0 nuevas, 16 actualizadas, 16 en total`: no se duplica nada.

## Cómo funciona el flujo

| Etapa | Qué hace |
|---|---|
| **Extract** | Lee `data/ventas_raw.csv` y valida que existan las columnas esperadas. |
| **Transform** | Recorta espacios y normaliza mayúsculas, convierte tipos, descarta filas con nulos, fechas inválidas o cantidades ≤ 0, elimina duplicados por `id_venta`, calcula `importe_total` y genera un resumen agregado por día, sucursal y categoría. |
| **Load** | Crea las tablas si no existen (`sql/schema.sql`) y carga los datos dentro de una transacción. |

## Patrón de datos utilizado

Se usa un patrón analítico simple de **tabla de hechos + tabla agregada**:

- **`ventas`** (hechos): una fila por venta limpia. `id_venta` es la clave primaria.
- **`resumen_ventas_diario`** (agregada): totales por `fecha`, `sucursal` y `categoria` (clave primaria compuesta), lista para reportes.

El DDL completo está en [`sql/schema.sql`](sql/schema.sql).

## Cómo se evita la duplicidad de datos

1. **`CREATE TABLE IF NOT EXISTS`**: el esquema se puede ejecutar en cada corrida sin error.
2. **Clave primaria** en `id_venta`: la propia base rechaza filas repetidas.
3. **UPSERT** (`INSERT ... ON CONFLICT(id_venta) DO UPDATE`): si la venta ya existe, se actualiza en lugar de insertarse de nuevo.
4. **Limpieza previa** de la tabla agregada (`DELETE` + `INSERT ... SELECT`): el resumen se recalcula siempre desde la tabla de hechos.
5. **Deduplicación en origen**: `drop_duplicates` elimina repetidos dentro del mismo CSV antes de cargar.

## Manejo de errores

- Archivo de origen inexistente, vacío o mal formado.
- Columnas faltantes en el origen.
- Fallas de conexión o permisos sobre la base SQLite (con `timeout` ante bloqueos).
- Errores durante la carga: se hace **rollback** y la base queda como estaba.

En todos los casos el script registra el error y termina con código de salida `1`, lo que permite integrarlo en un orquestador (cron, Task Scheduler, Airflow, etc.).

## Consultar los resultados

```bash
python -c "import sqlite3; c=sqlite3.connect('ventas.db'); [print(r) for r in c.execute('SELECT * FROM resumen_ventas_diario ORDER BY fecha')]"
```

O abrir `ventas.db` con cualquier cliente SQLite (DB Browser for SQLite, DBeaver, extensión de VS Code).

## Automatización programada (opcional)

Ejemplo con cron para correr el ETL todos los días a las 2 AM:

```
0 2 * * * cd /ruta/a/etl-ventas && .venv/bin/python main.py >> logs/etl.log 2>&1
```
