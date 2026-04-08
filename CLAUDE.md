# CLAUDE.md — Contexto del Proyecto ETL Violencia Intrafamiliar

> Este archivo es la memoria del agente ClaudeETL para este proyecto.
> Actualizar cada vez que se tomen decisiones importantes.

---

## Identificación del Proyecto

- **Estudiante:** Juan José Albán — 2235677
- **Curso:** ETL (G51) — Universidad Autónoma de Occidente
- **Docente:** Daniel Felipe Romero
- **Entrega:** Segunda entrega — Deadline: 7 de abril de 2026 a las 23:59
- **GitHub primera entrega:** https://github.com/alban0918/ETL-Violencia-Intrafamiliar

---

## Objetivo del Proyecto

Analizar la violencia intrafamiliar en Colombia mediante un pipeline ETL automatizado con:
- **ODS 5:** Igualdad de Género
- **ODS 16:** Paz, Justicia e Instituciones Sólidas

---

## Stack Tecnológico

| Capa | Herramienta |
|---|---|
| Orquestación | Apache Airflow |
| Extracción | Python (requests, BeautifulSoup, pandas) |
| Transformación | Python (pandas, unidecode) |
| Validación | Great Expectations 0.18.x |
| Data Warehouse | PostgreSQL en Supabase |
| Visualización | Power BI |
| Documentación | GitHub + Markdown |

---

## Fuentes de Datos

### Fuente 1 — Violencia Intrafamiliar (CSV, datos.gov.co)
- Dataset: "Reporte Delito Violencia Intrafamiliar – Policía Nacional"
- URL: https://www.datos.gov.co/resource/sqer-ipyf.csv
- Archivo procesado: `data/raw/01_violencia_raw_limpia.csv`
- Columnas: departamento, municipio, codigo_dane, armas_medios, fecha_hecho, genero, grupo_etario, cantidad, anio
- Registros: ~660,000+

### Fuente 2 — DANE IPM Departamental (Web Scraping + Excel)
- URL base: https://www.dane.gov.co/index.php/estadisticas-por-tema/pobreza-y-condiciones-de-vida/pobreza-multidimensional
- Archivo objetivo: anex-PMultidimensional-Departamental-2024.xlsx (hoja: IPM_Departamentos)
- Archivo procesado: `data/raw/02_dane_ipm_long.csv`
- Columnas: departamento, anio, ipm_total
- Registros: 231

### Fuente 3 — API DIVIPOLA (API pública datos.gov.co)
- Endpoint: https://www.datos.gov.co/resource/gdxc-w37w.json?$limit=5000
- Archivo procesado: `data/raw/03_api_divipola.csv`
- Columnas: cod_dpto, dpto, cod_mpio, nom_mpio, tipo_municipio, longitud, latitud

### Dataset Final Enriquecido
- Archivo: `data/final/04_dataset_final_enriquecido.csv`
- 511 registros x 10 columnas
- Columnas: cod_dpto, dpto, departamento_limpio, anio, total_casos, casos_femenino, casos_masculino, casos_adultos, casos_menores, ipm_total
- ipm_total tiene 287 nulos (cobertura temporal del DANE no cubre todos los años/departamentos — validado con tolerancia mostly=0.40)

---

## Estructura del Repositorio

```
etl-violencia-intrafamiliar/
├── dags/
│   └── etl_violencia_pipeline.py    # DAG principal de Airflow
├── src/
│   ├── config.py                    # Variables y rutas centralizadas
│   ├── extract.py                   # Extracción de las 3 fuentes
│   ├── transform.py                 # Transformación y merge
│   ├── validate.py                  # Great Expectations
│   └── load.py                      # Carga a Supabase/PostgreSQL
├── data/
│   ├── raw/                         # Datos originales descargados
│   ├── processed/                   # Datos intermedios
│   └── final/                       # Dataset enriquecido listo
├── notebooks/                       # Notebooks de Colab (evidencia EDA)
├── docs/
│   └── informe_tecnico_segunda_entrega.md
├── great_expectations/              # Configuración GE
├── tests/
│   └── test_pipeline.py
├── logs/
├── CLAUDE.md
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
└── run_pipeline.py                  # Ejecutor local sin Airflow
```

---

## Decisiones Técnicas

### 1. Great Expectations versión 0.18.x
Se usa `gx.from_pandas(df)` (API legacy simplificada). Compatible con Python 3.12 y funcional en Colab. Se confirma que todas las 13 validaciones pasan. El ipm_total tiene mostly=0.40 por cobertura temporal del DANE.

### 2. Supabase como Data Warehouse
Ya usado en la primera entrega. Credenciales via variables de entorno. Las tablas del DW son:
- `fact_violencia` (tabla de hechos, star schema)
- `dim_tiempo` (dimensión temporal)
- `dim_departamento` (dimensión geográfica con coords DIVIPOLA)
- `dim_genero`, `dim_grupo_etario` (dimensiones simples)

### 3. Airflow en Windows
Airflow no soporta Windows nativamente. Para la entrega se provee:
- El DAG correctamente escrito en `dags/etl_violencia_pipeline.py`
- Un script alternativo `run_pipeline.py` para ejecutar sin Airflow
- README con instrucciones para WSL2 o Docker si se requiere demo completa

### 4. Homologación territorial
Mapa de nombres aplicado antes del merge:
- "GUAJIRA" → "LA GUAJIRA"
- "SAN ANDRES" → "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA"
- "VALLE" → "VALLE DEL CAUCA"
- "BOGOTA D.C." → "BOGOTA, D.C."

### 5. Variables de entorno
Nunca hardcodear credenciales. Usar `.env` con python-dotenv. El `.env.example` muestra la estructura sin valores reales.

---

## Reglas de Trabajo

1. No hardcodear rutas absolutas — usar `BASE_DIR` relativo en `config.py`
2. No hardcodear credenciales — siempre usar `os.environ` o `.env`
3. No modificar los notebooks originales — solo leer para extraer lógica
4. Cada script debe poder ejecutarse de forma independiente
5. Los logs van en `logs/` con timestamp
6. Airflow DAG usa `PythonOperator` para invocar funciones de `src/`
7. La carga solo ocurre si `validate.py` retorna `True`
