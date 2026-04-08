"""
config.py — Configuración centralizada del pipeline ETL.
Todas las rutas, URLs y parámetros se definen aquí.
Las credenciales se leen desde variables de entorno (.env).
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# ─── Rutas base ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
FINAL_DIR = DATA_DIR / "final"
LOGS_DIR = BASE_DIR / "logs"

# Crear directorios si no existen
for d in [RAW_DIR, PROCESSED_DIR, FINAL_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ─── Archivos de datos ─────────────────────────────────────────────────────────
VIOLENCIA_RAW_FILE = RAW_DIR / "01_violencia_raw_limpia.csv"
DANE_IPM_FILE = RAW_DIR / "02_dane_ipm_long.csv"
DIVIPOLA_FILE = RAW_DIR / "03_api_divipola.csv"
DATASET_FINAL_FILE = FINAL_DIR / "04_dataset_final_enriquecido.csv"
DANE_EXCEL_FILE = PROCESSED_DIR / "ipm_dane_departamental.xlsx"

# ─── URLs y APIs ───────────────────────────────────────────────────────────────
load_dotenv(BASE_DIR / ".env")

DIVIPOLA_API_URL = os.getenv(
    "DIVIPOLA_API_URL",
    "https://www.datos.gov.co/resource/gdxc-w37w.json"
)
DIVIPOLA_LIMIT = int(os.getenv("DIVIPOLA_LIMIT", "5000"))

DANE_URL = (
    "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
    "pobreza-y-condiciones-de-vida/pobreza-multidimensional"
)
DANE_SHEET = "IPM_Departamentos"

VIOLENCIA_CSV_URL = (
    "https://www.datos.gov.co/api/views/sqer-ipyf/rows.csv"
    "?accessType=DOWNLOAD"
)

# ─── Base de datos ─────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "aws-0-us-east-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def get_db_connection_string() -> str:
    """Retorna la cadena de conexión PostgreSQL desde variables de entorno."""
    if not DB_USER or not DB_PASSWORD:
        raise EnvironmentError(
            "Faltan DB_USER y/o DB_PASSWORD en las variables de entorno. "
            "Copia .env.example como .env y completa las credenciales."
        )
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ─── Homologación de nombres departamentales ───────────────────────────────────
HOMOLOGACION_DEPARTAMENTOS = {
    "GUAJIRA": "LA GUAJIRA",
    "SAN ANDRES": "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
    "VALLE": "VALLE DEL CAUCA",
    "BOGOTA D.C.": "BOGOTA, D.C.",
    "BOGOTA DC": "BOGOTA, D.C.",
}

# ─── Logging ───────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

def setup_logging(name: str = "etl") -> logging.Logger:
    """Configura y retorna un logger con salida a consola y archivo."""
    import datetime
    log_file = LOGS_DIR / f"{name}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=LOG_DATEFMT,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger(name)
