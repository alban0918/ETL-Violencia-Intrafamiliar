"""
run_pipeline.py — Ejecutor local del pipeline ETL sin necesidad de Airflow.

Útil para:
- Probar el pipeline completo en Windows sin instalar Airflow
- Demo rápida para la entrega del proyecto
- Validar que todos los scripts funcionan correctamente

Uso:
    python run_pipeline.py             # Ejecuta el pipeline completo
    python run_pipeline.py --skip-load # Ejecuta sin cargar al DW (útil si no tienes .env)
    python run_pipeline.py --only-validate # Solo valida el dataset final existente
"""

import sys
import argparse
import time
from pathlib import Path

# ─── Setup de path ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from src.config import setup_logging

logger = setup_logging("pipeline")


def run_pipeline(skip_load: bool = False, only_validate: bool = False):
    """Ejecuta el pipeline ETL completo de forma secuencial."""

    logger.info("=" * 60)
    logger.info("  PIPELINE ETL — Violencia Intrafamiliar en Colombia")
    logger.info("  Curso ETL G51 — Universidad Autónoma de Occidente")
    logger.info("  Estudiante: Juan José Albán")
    logger.info("=" * 60)

    tiempos = {}
    inicio_total = time.time()

    # ── Paso 5: Solo validar ──────────────────────────────────────────────────
    if only_validate:
        logger.info("\n[MODO] Solo validación del dataset existente")
        t0 = time.time()
        from src.validate import run_validate
        run_validate()
        tiempos["validate"] = round(time.time() - t0, 2)
        _mostrar_resumen(tiempos, inicio_total)
        return

    # ── Paso 1: Extraer violencia ─────────────────────────────────────────────
    logger.info("\n[PASO 1/5] Extrayendo: Violencia Intrafamiliar (CSV)")
    t0 = time.time()
    from src.extract import extract_violencia
    df_violencia = extract_violencia()
    tiempos["extract_violencia"] = round(time.time() - t0, 2)
    logger.info(f"  → {df_violencia.shape[0]:,} registros extraídos en {tiempos['extract_violencia']}s")

    # ── Paso 2: Extraer DANE ──────────────────────────────────────────────────
    logger.info("\n[PASO 2/5] Extrayendo: DANE IPM Departamental")
    t0 = time.time()
    from src.extract import extract_dane_ipm
    df_dane = extract_dane_ipm()
    tiempos["extract_dane"] = round(time.time() - t0, 2)
    logger.info(f"  → {df_dane.shape[0]} registros extraídos en {tiempos['extract_dane']}s")

    # ── Paso 3: Extraer API DIVIPOLA ──────────────────────────────────────────
    logger.info("\n[PASO 3/5] Extrayendo: API DIVIPOLA")
    t0 = time.time()
    from src.extract import extract_divipola
    df_divipola = extract_divipola()
    tiempos["extract_api"] = round(time.time() - t0, 2)
    logger.info(f"  → {df_divipola.shape[0]} registros extraídos en {tiempos['extract_api']}s")

    # ── Paso 4: Transformar ───────────────────────────────────────────────────
    logger.info("\n[PASO 4/5] Transformando y uniendo las 3 fuentes...")
    t0 = time.time()
    from src.transform import run_transform
    df_final = run_transform(df_violencia, df_dane, df_divipola)
    tiempos["transform"] = round(time.time() - t0, 2)
    logger.info(f"  → Dataset final: {df_final.shape} en {tiempos['transform']}s")

    # ── Paso 5: Validar ───────────────────────────────────────────────────────
    logger.info("\n[PASO 5/6] Validando con Great Expectations...")
    t0 = time.time()
    from src.validate import run_validate
    try:
        validacion_ok = run_validate(df_final)
        tiempos["validate"] = round(time.time() - t0, 2)
        logger.info(f"  → Validación APROBADA en {tiempos['validate']}s")
    except RuntimeError as e:
        tiempos["validate"] = round(time.time() - t0, 2)
        logger.error(f"  ✗ Validación FALLIDA: {e}")
        logger.error("  → Pipeline detenido. El dataset NO se cargará.")
        _mostrar_resumen(tiempos, inicio_total, success=False)
        sys.exit(1)

    # ── Paso 6: Cargar al DW ──────────────────────────────────────────────────
    if skip_load:
        logger.info("\n[PASO 6/6] Carga omitida (--skip-load activado)")
        logger.info("  Para cargar al DW, crea el archivo .env con tus credenciales")
        logger.info("  y ejecuta: python run_pipeline.py")
    else:
        logger.info("\n[PASO 6/6] Cargando al Data Warehouse (Supabase/PostgreSQL)...")
        t0 = time.time()
        from src.load import run_load
        run_load(df_final)
        tiempos["load"] = round(time.time() - t0, 2)
        logger.info(f"  → Carga completada en {tiempos['load']}s")

    _mostrar_resumen(tiempos, inicio_total, success=True)


def _mostrar_resumen(tiempos: dict, inicio: float, success: bool = True):
    total = round(time.time() - inicio, 2)
    logger.info("\n" + "=" * 60)
    logger.info("  RESUMEN DEL PIPELINE")
    logger.info("=" * 60)
    for paso, t in tiempos.items():
        logger.info(f"  {paso:<25} {t:>6.2f}s")
    logger.info(f"  {'TOTAL':<25} {total:>6.2f}s")
    logger.info("=" * 60)
    estado = "EXITOSO" if success else "FALLIDO"
    logger.info(f"  RESULTADO: {estado}")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pipeline ETL — Violencia Intrafamiliar Colombia"
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Ejecutar pipeline sin cargar al Data Warehouse",
    )
    parser.add_argument(
        "--only-validate",
        action="store_true",
        help="Solo validar el dataset final existente",
    )
    args = parser.parse_args()
    run_pipeline(skip_load=args.skip_load, only_validate=args.only_validate)
