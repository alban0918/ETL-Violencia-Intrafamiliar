"""
validate.py — Validación de calidad de datos con Great Expectations 1.x.

Ejecuta 13 validaciones sobre el dataset final enriquecido.
Retorna True si todas las validaciones críticas pasan.
Lanza RuntimeError si alguna validación crítica falla (bloquea la carga al DW).

Compatibilidad:
- Python 3.13 local: great-expectations >= 1.x  (API fluent + context.data_sources)
- Google Colab (Py 3.12): puede usarse GE 0.18.x con gx.from_pandas() si se prefiere
"""

import logging

import pandas as pd
import great_expectations as gx

from src.config import DATASET_FINAL_FILE, setup_logging

logger = logging.getLogger(__name__)


def run_validate(df: pd.DataFrame | None = None) -> bool:
    """
    Valida el dataset final con Great Expectations (modo ephemeral).

    Args:
        df: DataFrame a validar. Si es None, carga desde DATASET_FINAL_FILE.

    Returns:
        True si todas las validaciones críticas pasan.

    Raises:
        RuntimeError: si alguna validación crítica falla.
    """
    logger.info("=== VALIDATE — Great Expectations %s ===", gx.__version__)

    if df is None:
        if not DATASET_FINAL_FILE.exists():
            raise FileNotFoundError(
                f"No se encontró el dataset final en: {DATASET_FINAL_FILE}\n"
                "Ejecuta primero transform.py"
            )
        logger.info("Cargando dataset desde: %s", DATASET_FINAL_FILE)
        df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})

    logger.info("Dataset cargado: %s", str(df.shape))
    logger.info("Nulos: %s", str(df.isnull().sum().to_dict()))

    # ─── Contexto + fuente de datos ──────────────────────────────────────────
    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_pandas("violencia_source")
    data_asset = datasource.add_dataframe_asset("dataset_final")
    batch_def = data_asset.add_batch_definition_whole_dataframe("batch_completo")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})

    # ─── Suite de expectativas ────────────────────────────────────────────────
    suite = context.suites.add(gx.ExpectationSuite(name="suite_violencia"))

    # ─── 1–5. No nulos en columnas críticas ──────────────────────────────────
    for col in ["cod_dpto", "dpto", "departamento_limpio", "anio", "total_casos"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
        )

    # ─── 6. Rango de años 2010–2025 ──────────────────────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="anio", min_value=2010, max_value=2025
        )
    )

    # ─── 7–11. Valores no negativos en conteos ────────────────────────────────
    for col in ["total_casos", "casos_femenino", "casos_masculino",
                "casos_adultos", "casos_menores"]:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0)
        )

    # ─── 12. IPM con tolerancia (cobertura parcial DANE — 287 nulos esperados) ─
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="ipm_total", min_value=0, max_value=100, mostly=0.40
        )
    )

    # ─── 13. Unicidad de la clave cod_dpto + anio ─────────────────────────────
    suite.add_expectation(
        gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=["cod_dpto", "anio"]
        )
    )

    # ─── Ejecutar validación ─────────────────────────────────────────────────
    logger.info("Ejecutando suite de validación...")
    validation_result = batch.validate(suite)

    # ─── Parsear resultados ───────────────────────────────────────────────────
    resultados = {}
    nombre_map = [
        "cod_dpto_no_nulo", "dpto_no_nulo", "departamento_limpio_no_nulo",
        "anio_no_nulo", "total_casos_no_nulo",
        "anio_rango",
        "total_casos_positivo", "casos_femenino_positivo", "casos_masculino_positivo",
        "casos_adultos_positivo", "casos_menores_positivo",
        "ipm_total_rango",
        "unicidad_cod_dpto_anio",
    ]

    results_list = validation_result.results
    for i, r in enumerate(results_list):
        nombre = nombre_map[i] if i < len(nombre_map) else f"check_{i}"
        resultados[nombre] = r.success

    # ─── Resumen en log ───────────────────────────────────────────────────────
    logger.info("\n" + "=" * 55)
    logger.info("RESUMEN DE VALIDACIONES — GE %s", gx.__version__)
    logger.info("=" * 55)
    for nombre, ok in resultados.items():
        logger.info("  [%s] %s", "PASS" if ok else "FAIL", nombre)
    logger.info("Resultado global: %s", "PASS" if validation_result.success else "FAIL")
    logger.info("=" * 55)

    # ─── Evaluar validaciones críticas (excluye ipm_total_rango) ─────────────
    validaciones_criticas = [
        "cod_dpto_no_nulo", "dpto_no_nulo", "departamento_limpio_no_nulo",
        "anio_no_nulo", "total_casos_no_nulo", "anio_rango",
        "total_casos_positivo", "casos_femenino_positivo", "casos_masculino_positivo",
        "casos_adultos_positivo", "casos_menores_positivo", "unicidad_cod_dpto_anio",
    ]
    fallidas = [k for k in validaciones_criticas if not resultados.get(k, False)]

    if fallidas:
        msg = f"VALIDACION FALLIDA — Checks críticos no superados: {fallidas}"
        logger.error(msg)
        raise RuntimeError(msg)

    if not resultados.get("ipm_total_rango", True):
        logger.warning("ipm_total_rango no pasó (no es crítica — cobertura parcial DANE)")

    logger.info("VALIDACION FINAL: APROBADA — Puede cargarse al Data Warehouse.")
    return True


# ─── Punto de entrada independiente ──────────────────────────────────────────

if __name__ == "__main__":
    setup_logging("validate")
    try:
        resultado = run_validate()
        print(f"\nResultado: {'APROBADO' if resultado else 'RECHAZADO'}")
    except RuntimeError as e:
        print(f"\nERROR: {e}")
        exit(1)
