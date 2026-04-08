"""
load.py — Carga del dataset validado al Data Warehouse (PostgreSQL / Supabase).

Modelo de datos: Star Schema
- fact_violencia        (tabla de hechos principal)
- dim_tiempo            (dimensión temporal)
- dim_departamento      (dimensión geográfica con código DIVIPOLA)
- dim_genero            (dimensión de género)
- dim_grupo_etario      (dimensión de grupo etario)

Las credenciales se leen exclusivamente desde variables de entorno (.env).
La carga solo debe ejecutarse si validate.py retornó True.
"""

import logging

import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DATASET_FINAL_FILE, get_db_connection_string, setup_logging

logger = logging.getLogger(__name__)

# ─── DDL de tablas ────────────────────────────────────────────────────────────

DDL_STATEMENTS = """
-- Dimensión temporal
CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo   SERIAL PRIMARY KEY,
    anio        INTEGER NOT NULL UNIQUE
);

-- Dimensión departamento (incluye código DANE/DIVIPOLA)
CREATE TABLE IF NOT EXISTS dim_departamento (
    id_departamento     SERIAL PRIMARY KEY,
    cod_dpto            VARCHAR(5) NOT NULL UNIQUE,
    dpto                VARCHAR(100),
    departamento_limpio VARCHAR(100) NOT NULL
);

-- Dimensión género
CREATE TABLE IF NOT EXISTS dim_genero (
    id_genero   SERIAL PRIMARY KEY,
    genero      VARCHAR(20) NOT NULL UNIQUE
);

-- Dimensión grupo etario
CREATE TABLE IF NOT EXISTS dim_grupo_etario (
    id_grupo_etario SERIAL PRIMARY KEY,
    grupo_etario    VARCHAR(30) NOT NULL UNIQUE
);

-- Tabla de hechos principal
CREATE TABLE IF NOT EXISTS fact_violencia (
    id_hecho            SERIAL PRIMARY KEY,
    id_departamento     INTEGER REFERENCES dim_departamento(id_departamento),
    id_tiempo           INTEGER REFERENCES dim_tiempo(id_tiempo),
    total_casos         INTEGER NOT NULL DEFAULT 0,
    casos_femenino      INTEGER NOT NULL DEFAULT 0,
    casos_masculino     INTEGER NOT NULL DEFAULT 0,
    casos_adultos       INTEGER NOT NULL DEFAULT 0,
    casos_menores       INTEGER NOT NULL DEFAULT 0,
    ipm_total           FLOAT,
    UNIQUE (id_departamento, id_tiempo)
);
"""


# ─── Carga de dimensiones ─────────────────────────────────────────────────────

def _upsert_dim_tiempo(engine, df: pd.DataFrame) -> dict:
    """Inserta años únicos en dim_tiempo. Retorna mapa anio → id_tiempo."""
    anios = df["anio"].dropna().unique().tolist()
    with engine.begin() as conn:
        for anio in anios:
            conn.execute(text(
                "INSERT INTO dim_tiempo (anio) VALUES (:anio) ON CONFLICT (anio) DO NOTHING"
            ), {"anio": int(anio)})

        rows = conn.execute(text("SELECT id_tiempo, anio FROM dim_tiempo")).fetchall()
    return {row.anio: row.id_tiempo for row in rows}


def _upsert_dim_departamento(engine, df: pd.DataFrame) -> dict:
    """Inserta departamentos únicos en dim_departamento. Retorna mapa cod_dpto → id_departamento."""
    deptos = (
        df[["cod_dpto", "dpto", "departamento_limpio"]]
        .dropna(subset=["cod_dpto"])
        .drop_duplicates(subset=["cod_dpto"])
    )
    with engine.begin() as conn:
        for _, row in deptos.iterrows():
            conn.execute(text("""
                INSERT INTO dim_departamento (cod_dpto, dpto, departamento_limpio)
                VALUES (:cod_dpto, :dpto, :departamento_limpio)
                ON CONFLICT (cod_dpto) DO UPDATE
                SET dpto = EXCLUDED.dpto,
                    departamento_limpio = EXCLUDED.departamento_limpio
            """), {
                "cod_dpto": str(row["cod_dpto"]),
                "dpto": str(row["dpto"]) if pd.notna(row["dpto"]) else None,
                "departamento_limpio": str(row["departamento_limpio"]),
            })
        rows = conn.execute(
            text("SELECT id_departamento, cod_dpto FROM dim_departamento")
        ).fetchall()
    return {row.cod_dpto: row.id_departamento for row in rows}


# ─── Carga de hechos ─────────────────────────────────────────────────────────

def _cargar_fact_violencia(engine, df: pd.DataFrame, mapa_tiempo: dict, mapa_dpto: dict) -> int:
    """Inserta registros en fact_violencia. Retorna cantidad de filas insertadas."""
    filas_insertadas = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            id_tiempo = mapa_tiempo.get(int(row["anio"])) if pd.notna(row["anio"]) else None
            id_dpto = mapa_dpto.get(str(row["cod_dpto"])) if pd.notna(row["cod_dpto"]) else None

            if id_tiempo is None or id_dpto is None:
                logger.warning(
                    f"Omitiendo fila sin dimensiones: "
                    f"anio={row['anio']}, cod_dpto={row['cod_dpto']}"
                )
                continue

            conn.execute(text("""
                INSERT INTO fact_violencia
                    (id_departamento, id_tiempo, total_casos, casos_femenino,
                     casos_masculino, casos_adultos, casos_menores, ipm_total)
                VALUES
                    (:id_dpto, :id_tiempo, :total_casos, :casos_femenino,
                     :casos_masculino, :casos_adultos, :casos_menores, :ipm_total)
                ON CONFLICT (id_departamento, id_tiempo) DO UPDATE SET
                    total_casos     = EXCLUDED.total_casos,
                    casos_femenino  = EXCLUDED.casos_femenino,
                    casos_masculino = EXCLUDED.casos_masculino,
                    casos_adultos   = EXCLUDED.casos_adultos,
                    casos_menores   = EXCLUDED.casos_menores,
                    ipm_total       = EXCLUDED.ipm_total
            """), {
                "id_dpto": id_dpto,
                "id_tiempo": id_tiempo,
                "total_casos": int(row["total_casos"]),
                "casos_femenino": int(row["casos_femenino"]),
                "casos_masculino": int(row["casos_masculino"]),
                "casos_adultos": int(row["casos_adultos"]),
                "casos_menores": int(row["casos_menores"]),
                "ipm_total": float(row["ipm_total"]) if pd.notna(row["ipm_total"]) else None,
            })
            filas_insertadas += 1

    return filas_insertadas


# ─── Pipeline de carga ────────────────────────────────────────────────────────

def run_load(df: pd.DataFrame | None = None) -> None:
    """
    Carga el dataset validado al Data Warehouse en Supabase/PostgreSQL.

    Args:
        df: DataFrame validado. Si es None, carga desde DATASET_FINAL_FILE.

    Raises:
        EnvironmentError: si faltan credenciales de base de datos.
        FileNotFoundError: si no existe el dataset final.
    """
    logger.info("=== LOAD — Iniciando carga al Data Warehouse ===")

    if df is None:
        if not DATASET_FINAL_FILE.exists():
            raise FileNotFoundError(
                f"No se encontró el dataset en: {DATASET_FINAL_FILE}"
            )
        logger.info(f"Cargando dataset desde: {DATASET_FINAL_FILE}")
        df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})

    logger.info(f"Dataset a cargar: {df.shape}")

    conn_str = get_db_connection_string()
    logger.info("Conectando al Data Warehouse...")
    engine = create_engine(conn_str)

    # Verificar conexión
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Conexión exitosa al Data Warehouse.")

    # Crear tablas si no existen
    logger.info("Creando tablas (si no existen)...")
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    logger.info("Tablas verificadas/creadas.")

    # Cargar dimensiones
    logger.info("Cargando dim_tiempo...")
    mapa_tiempo = _upsert_dim_tiempo(engine, df)
    logger.info(f"  {len(mapa_tiempo)} años en dim_tiempo")

    logger.info("Cargando dim_departamento...")
    mapa_dpto = _upsert_dim_departamento(engine, df)
    logger.info(f"  {len(mapa_dpto)} departamentos en dim_departamento")

    # Cargar hechos
    logger.info("Cargando fact_violencia...")
    n = _cargar_fact_violencia(engine, df, mapa_tiempo, mapa_dpto)
    logger.info(f"  {n} registros insertados/actualizados en fact_violencia")

    logger.info("=== LOAD — Carga completada exitosamente ===")


# ─── Punto de entrada independiente ──────────────────────────────────────────

if __name__ == "__main__":
    setup_logging("load")
    logger.info("Ejecutando carga directa (asegúrate de que el .env esté configurado)")
    run_load()
