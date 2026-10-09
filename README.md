# ETL de Ventas E-Commerce → SQLite (SQLAlchemy 2.0)

Flujo ETL (Extract, Transform, Load) automatizado en Python que toma las transacciones del dataset **[E-Commerce Data](https://www.kaggle.com/datasets/carrie1/ecommerce-data)** de Kaggle (facturas de un retailer online del Reino Unido, 2010-2011), las limpia y agrega, y las persiste en una base de datos analítica local (SQLite, o MySQL de forma opcional). La capa de persistencia está construida con **SQLAlchemy 2.0**. El proceso es **repetible**: se puede ejecutar todas las veces que se quiera sin duplicar datos.

## Estructura del proyecto

```
etl-ventas/
├── data/
│   ├── muestra_ecommerce.csv   # Muestra con el formato del dataset (incluida en el repo)
│   └── data.csv                # Dataset completo de Kaggle (se descarga aparte, no se versiona)
├── sql/
│   └── schema.sql              # DDL de referencia de las tablas de destino
├── main.py                     # Punto de entrada: ejecuta el flujo completo
├── etl.py                      # Lógica ETL (extraer, transformar, cargar)
├── models.py                   # Modelos ORM de SQLAlchemy 2.0 (tablas de destino)
├── requirements.txt            # Dependencias de Python
├── .gitignore
└── README.md
```

## Datos de origen

El archivo `data.csv` del dataset tiene unas 540.000 líneas de factura con estas columnas:

| Columna | Descripción | Columna en la base |
|---|---|---|
| `InvoiceNo` | Número de factura (si empieza con "C" es una cancelación) | `factura` |
| `StockCode` | Código del producto | `codigo_producto` |
| `Description` | Nombre del producto | `descripcion` |
| `Quantity` | Unidades | `cantidad` |
| `InvoiceDate` | Fecha y hora (formato `M/D/AAAA H:MM`) | `fecha_factura` |
| `UnitPrice` | Precio unitario en libras | `precio_unitario` |
| `CustomerID` | Identificador del cliente (puede venir vacío) | `id_cliente` |
| `Country` | País del cliente | `pais` |

El archivo viene en codificación **ISO-8859-1** (no UTF-8) y pesa unos 45 MB, por eso no se sube al repositorio. Para probar el flujo sin descargarlo, el repo incluye `data/muestra_ecommerce.csv`: unas pocas filas de ejemplo con el mismo formato, que incluyen a propósito los problemas típicos del dataset (duplicados, cancelaciones, clientes vacíos, fechas inválidas, cantidades negativas).

## Requisitos

- Python 3.10 o superior
- pip
- Dependencias (ver `requirements.txt`):
  - `pandas` para la extracción y transformación
  - `SQLAlchemy` 2.x para la capa de persistencia
- SQLite viene incluido en Python, no hace falta instalar un servidor de base de datos.
- Solo si se usa MySQL: un servidor MySQL y el driver `pymysql` (`pip install pymysql`).
- Para el dataset completo: una cuenta de Kaggle.

## Pasos para ejecutar

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/Diegol2022/etl-ventas.git
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
4. Descargar el dataset desde [Kaggle](https://www.kaggle.com/datasets/carrie1/ecommerce-data) (botón **Download**), descomprimirlo y copiar `data.csv` a la carpeta `data/`.
   Si se omite este paso, el script usa automáticamente la muestra incluida.
5. Ejecutar el ETL:
   ```bash
   python main.py
   ```
   Se genera (o actualiza) el archivo `ecommerce.db` en la raíz del proyecto.

6. (Opcional) Usar otro origen o destino:
   ```bash
   python main.py --origen data/muestra_ecommerce.csv --destino salida/analitica.db
   ```

7. (Opcional) Cargar en MySQL en lugar de SQLite, pasando una URL de SQLAlchemy:
   ```bash
   python main.py --db-url "mysql+pymysql://usuario:clave@localhost:3306/ecommerce"
   ```
   También se puede definir la variable de entorno `DATABASE_URL` con esa URL. La base (`ecommerce`) tiene que existir; las tablas las crea el script.

Salida esperada con la muestra:

```
[WARNING] No se encontró data.csv (ver README para descargarlo). Se usa la muestra muestra_ecommerce.csv
[INFO] Extrayendo datos desde .../data/muestra_ecommerce.csv
[INFO] Filas extraídas: 32
[INFO] Filas válidas tras limpieza: 23 de 32
[INFO]   - duplicados exactos: 1
[INFO]   - facturas canceladas: 2
[INFO]   - códigos no producto: 1
[INFO]   - sin cliente: 2
[INFO]   - nulos o fechas inválidas: 1
[INFO]   - cantidad o precio <= 0: 1
[INFO]   - líneas consolidadas: 1
[INFO] Filas del resumen diario: 9
[INFO] Ventas: 23 nuevas, 0 actualizadas, 23 en total
[INFO] Resumen diario recalculado: 9 filas
[INFO] ETL finalizado OK -> sqlite:///.../ecommerce.db
```

Si se vuelve a ejecutar, informa `0 nuevas, 23 actualizadas, 23 en total`: no se duplica nada. Con el dataset completo los números son mayores y la carga tarda más (procesa en lotes de 5.000 filas).

## Cómo funciona el flujo

| Etapa | Qué hace |
|---|---|
| **Extract** | Lee el CSV con pandas en ISO-8859-1, valida que existan las columnas esperadas y las renombra al español. |
| **Transform** | Limpia y valida los datos (ver detalle abajo) y genera un resumen agregado por día y país. |
| **Load** | Con SQLAlchemy: crea el `Engine`, verifica la conexión, crea las tablas desde los modelos si no existen y carga los datos en una única transacción. |

Reglas de limpieza, en orden:

1. Recorta espacios, unifica mayúsculas y espacios repetidos en los textos.
2. Elimina **duplicados exactos** (el dataset trae varios miles).
3. Descarta **facturas canceladas** (`InvoiceNo` que empieza con "C").
4. Descarta **códigos que no son productos**: gastos de envío (`POST`, `DOT`), descuentos (`D`), ajustes manuales (`M`), comisiones bancarias, etc.
5. Convierte tipos y descarta ventas **sin cliente**, con **nulos**, **fechas inválidas**, o **cantidad o precio ≤ 0**.
6. **Consolida** el mismo producto repetido dentro de una factura sumando las cantidades, para que `(factura, codigo_producto)` sea una clave única.

## Capa de persistencia (SQLAlchemy 2.0)

- **Modelos** (`models.py`): las tablas se definen con el estilo declarativo tipado de la versión 2.0 (`DeclarativeBase`, `Mapped`, `mapped_column`), incluyendo claves primarias, restricciones `CHECK` e índices.
- **Creación del esquema**: `Base.metadata.create_all(engine)` crea solo las tablas que no existen (equivale a `CREATE TABLE IF NOT EXISTS`).
- **Conexión**: `create_engine()` + una consulta `SELECT 1` para detectar errores de conexión antes de empezar a cargar.
- **Transacción**: la carga corre dentro de `with Session(engine) as session, session.begin():`. Si algo falla, SQLAlchemy hace rollback automáticamente.
- **Independencia del motor**: el mismo código funciona con SQLite o MySQL cambiando solo la URL de conexión; SQLAlchemy genera el SQL de cada motor.

## Patrón de datos utilizado

Se usa un patrón analítico simple de **tabla de hechos + tabla agregada**:

- **`ventas`** (hechos): una fila por producto dentro de cada factura. Clave primaria compuesta `(factura, codigo_producto)`, más índices por fecha y país.
- **`resumen_ventas_diario`** (agregada): cantidad de facturas, unidades e importe total por `fecha` y `pais` (clave primaria compuesta), lista para reportes.

Las tablas están definidas en [`models.py`](models.py). El DDL equivalente está en [`sql/schema.sql`](sql/schema.sql), que también sirve para crear el esquema a mano:

```bash
sqlite3 ecommerce.db < sql/schema.sql
```

## Cómo se evita la duplicidad de datos

1. **Creación condicional del esquema**: `create_all()` (y `IF NOT EXISTS` en `schema.sql`) permite ejecutar el proceso en cada corrida sin error.
2. **Clave primaria compuesta** `(factura, codigo_producto)`: la propia base rechaza filas repetidas.
3. **UPSERT**: si la línea ya existe, se actualiza en lugar de insertarse de nuevo. SQLAlchemy genera `INSERT ... ON CONFLICT DO UPDATE` en SQLite y `INSERT ... ON DUPLICATE KEY UPDATE` en MySQL.
4. **Limpieza previa** de la tabla agregada (`DELETE` + `INSERT ... SELECT`): el resumen se recalcula siempre desde la tabla de hechos.
5. **Deduplicación en origen**: se eliminan los duplicados exactos del CSV y se consolidan las líneas repetidas antes de cargar.

## Manejo de errores

- Archivo de origen inexistente, vacío o mal formado.
- Columnas faltantes en el origen.
- URL de conexión inválida o driver de base de datos no instalado.
- Fallas de conexión a la base (detectadas con `SELECT 1` antes de cargar).
- Errores durante la carga: se hace **rollback** y la base queda como estaba.

En todos los casos el script registra el error y termina con código de salida `1`, lo que permite integrarlo en un orquestador (cron, Task Scheduler, Airflow, etc.).

## Consultar los resultados

```bash
python -c "from sqlalchemy import create_engine, text; e=create_engine('sqlite:///ecommerce.db'); [print(r) for r in e.connect().execute(text('SELECT * FROM resumen_ventas_diario ORDER BY importe_total DESC LIMIT 10'))]"
```

O abrir `ecommerce.db` con cualquier cliente SQLite (DB Browser for SQLite, DBeaver, extensión de VS Code).

## Automatización programada (opcional)

Ejemplo con cron para correr el ETL todos los días a las 2 AM:

```
0 2 * * * cd /ruta/a/etl-ventas && .venv/bin/python main.py >> logs/etl.log 2>&1
```
