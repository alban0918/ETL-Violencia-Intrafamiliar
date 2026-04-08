# ETL — Violencia Intrafamiliar en Colombia

**Curso:** ETL (G51) — Universidad Autónoma de Occidente  
**Estudiante:** Juan José Albán — 2235677  
**Docente:** Daniel Felipe Romero  
**Segunda Entrega — Abril 2026**

---

## Descripción

Pipeline ETL automatizado que analiza la violencia intrafamiliar en Colombia cruzando tres fuentes de datos públicas:

1. **Registros de delitos** — Policía Nacional (datos.gov.co)
2. **Índice de Pobreza Multidimensional (IPM)** — DANE por departamento y año
3. **División político-administrativa** — API DIVIPOLA (datos.gov.co)

El pipeline extrae, transforma, valida con Great Expectations y carga el dataset enriquecido a un Data Warehouse en PostgreSQL (Supabase).

**ODS alineados:** ODS 5 (Igualdad de Género) · ODS 16 (Paz, Justicia e Instituciones Sólidas)

---

## Arquitectura del Pipeline

```
[CSV Policía Nacional]  ──┐
                          │
[DANE IPM (Scraping)]  ───┼──► TRANSFORM ──► VALIDATE ──► LOAD (Supabase)
                          │      (pandas)      (GE)      (PostgreSQL)
[API DIVIPOLA]         ───┘

Orquestación: Apache Airflow DAG (@daily)
```

### Diagrama de tareas del DAG

```
extract_violencia ─┐
extract_dane      ─┼─► transform ─► validate ─► load
extract_api       ─┘
```

---

## Stack Tecnológico

| Capa | Herramienta |
|---|---|
| Orquestación | Apache Airflow 2.x |
| Extracción | Python + pandas + requests + BeautifulSoup |
| Transformación | Python + pandas + unidecode |
| Validación | Great Expectations 0.18.x |
| Data Warehouse | PostgreSQL / Supabase |
| Visualización | Power BI |
| Control de versiones | GitHub |

---

## Estructura del Repositorio

```
etl-violencia-intrafamiliar/
├── dags/
│   └── etl_violencia_pipeline.py    # DAG de Airflow (6 tareas)
├── src/
│   ├── config.py                    # Rutas, URLs, variables de entorno
│   ├── extract.py                   # Extracción de las 3 fuentes
│   ├── transform.py                 # Transformación y merge
│   ├── validate.py                  # Great Expectations (13 validaciones)
│   └── load.py                      # Carga a Supabase (Star Schema)
├── data/
│   ├── raw/                         # CSVs originales por fuente
│   └── final/                       # Dataset enriquecido (511 × 10)
├── notebooks/                       # Notebooks de exploración (Colab)
├── docs/
│   └── informe_tecnico_segunda_entrega.md
├── tests/
│   └── test_pipeline.py
├── run_pipeline.py                  # Ejecutor local (sin Airflow)
├── requirements.txt
├── .env.example
├── .gitignore
└── CLAUDE.md
```

---

## Instalación

### 1. Clonar el repositorio
```bash
git clone https://github.com/alban0918/ETL-Violencia-Intrafamiliar.git
cd ETL-Violencia-Intrafamiliar
```

### 2. Crear entorno virtual e instalar dependencias
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. Configurar variables de entorno
```bash
cp .env.example .env
# Editar .env con tus credenciales de Supabase
```

Contenido del `.env`:
```env
DB_HOST=aws-0-us-east-1.pooler.supabase.com
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres.TUPROYECTOID
DB_PASSWORD=tu_password_aqui
```

---

## Ejecución del Pipeline

### Opción A — Sin Airflow (recomendado para pruebas locales en Windows)

```bash
# Pipeline completo (extrae + transforma + valida + carga)
python run_pipeline.py

# Sin carga al DW (para probar sin credenciales)
python run_pipeline.py --skip-load

# Solo validar el dataset existente
python run_pipeline.py --only-validate
```

### Opción B — Con Airflow (Linux / WSL2 / Docker)

#### Instalación de Airflow
```bash
# En WSL2 o Linux
pip install apache-airflow==2.10.4 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"

# Inicializar base de datos de Airflow
export AIRFLOW_HOME=$(pwd)
airflow db init

# Crear usuario admin
airflow users create \
  --username admin \
  --firstname Juan \
  --lastname Alban \
  --role Admin \
  --email admin@uao.edu.co \
  --password admin
```

#### Iniciar Airflow (modo standalone — recomendado para desarrollo)
```bash
export AIRFLOW_HOME=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
airflow standalone
```
Acceder en: http://localhost:8080

#### O iniciar webserver y scheduler por separado
```bash
# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler
```

#### Ubicación del DAG
El DAG `etl_violencia_intrafamiliar` debe aparecer automáticamente en la UI de Airflow una vez que `dags/etl_violencia_pipeline.py` esté en la carpeta `dags/`.

#### Ejecutar el DAG manualmente
```bash
airflow dags trigger etl_violencia_intrafamiliar
```

---

## Ejecutar Pruebas

```bash
python tests/test_pipeline.py
```

---

## Dataset Final

El dataset `data/final/04_dataset_final_enriquecido.csv` contiene:

| Columna | Tipo | Descripción |
|---|---|---|
| cod_dpto | str | Código DANE del departamento (2 dígitos) |
| dpto | str | Nombre oficial del departamento |
| departamento_limpio | str | Nombre normalizado (sin tildes) |
| anio | int | Año del registro |
| total_casos | int | Total de casos de violencia intrafamiliar |
| casos_femenino | int | Casos con víctima femenina |
| casos_masculino | int | Casos con víctima masculina |
| casos_adultos | int | Casos con víctima adulta |
| casos_menores | int | Casos con víctima menor de edad |
| ipm_total | float | Índice de Pobreza Multidimensional (%) |

**Dimensiones:** 511 registros × 10 columnas

---

## Modelo de Datos (Star Schema)

```
              dim_tiempo
              (id_tiempo, anio)
                    │
dim_departamento ───┤
(id_departamento,   ├─── fact_violencia
 cod_dpto,          │    (total_casos, casos_femenino,
 dpto,              │     casos_masculino, casos_adultos,
 depto_limpio)      │     casos_menores, ipm_total)
                    │
              [FK → dim_tiempo]
              [FK → dim_departamento]
```

---

## Validaciones con Great Expectations

| Validación | Tipo | Resultado |
|---|---|---|
| cod_dpto no nulo | Crítica | PASS |
| dpto no nulo | Crítica | PASS |
| departamento_limpio no nulo | Crítica | PASS |
| anio no nulo | Crítica | PASS |
| total_casos no nulo | Crítica | PASS |
| anio en rango 2010–2025 | Crítica | PASS |
| total_casos ≥ 0 | Crítica | PASS |
| casos_femenino ≥ 0 | Crítica | PASS |
| casos_masculino ≥ 0 | Crítica | PASS |
| casos_adultos ≥ 0 | Crítica | PASS |
| casos_menores ≥ 0 | Crítica | PASS |
| unicidad cod_dpto + anio | Crítica | PASS |
| ipm_total en [0,100] (mostly=0.40) | Tolerante | PASS |

> La validación de `ipm_total` usa `mostly=0.40` porque el DANE no publica IPM para todos los departamentos en todos los años. Los 287 nulos son esperados.

---

## Decisiones Técnicas

1. **API DIVIPOLA**: Elegida por ser la fuente oficial del DANE para la división político-administrativa de Colombia. Provee coordenadas geográficas y códigos únicos por municipio/departamento, permitiendo homologar nombres entre fuentes.

2. **Homologación territorial**: Se aplicó un mapa explícito para casos problemáticos (GUAJIRA → LA GUAJIRA, SAN ANDRES → SAN ANDRES, PROVIDENCIA Y SANTA CATALINA, etc.) porque los tres datasets usan convenciones de nombres distintas.

3. **Star Schema**: Se eligió sobre snowflake por su simplicidad para consultas en Power BI. El nivel de granularidad es departamento × año, suficiente para los análisis requeridos.

4. **GE mostly=0.40 para IPM**: El DANE solo publica IPM desde 2018 para algunos departamentos. Los nulos no son errores de calidad sino ausencia de dato fuente. Se documentó explícitamente.

5. **Airflow en Windows**: Airflow no soporta Windows de forma nativa. Se incluye el DAG para demostrar la arquitectura y se provee `run_pipeline.py` como alternativa local funcional.

---

## Fuentes de Datos

| Fuente | URL | Tipo |
|---|---|---|
| Violencia Intrafamiliar | datos.gov.co/resource/sqer-ipyf | CSV descarga directa |
| DANE IPM Departamental | dane.gov.co/.../pobreza-multidimensional | Web scraping + Excel |
| DIVIPOLA | datos.gov.co/resource/gdxc-w37w | API REST JSON |

---

## Visualizaciones

Las visualizaciones se construyen en Power BI conectado directamente a la base de datos Supabase. Incluyen:

- Evolución anual de casos 2010–2025
- Top 10 departamentos con más casos
- Distribución por género (74.8% femenino)
- Distribución por grupo etario (91.5% adultos)
- Correlación IPM vs casos de violencia
- Mapa coroplético por departamento

---

## Licencia

Proyecto académico — Universidad Autónoma de Occidente — 2026
