# -*- coding: utf-8 -*-
"""
etl_violencia_pipeline.py - DAG principal de Apache Airflow.

Pipeline ETL: Violencia Intrafamiliar en Colombia
Curso: ETL G51 - Universidad Autonoma de Occidente
Estudiante: Juan Jose Alban

Tareas del DAG:
1. extract_violencia  - Extrae CSV Policia Nacional desde datos.gov.co
2. extract_dane       - Extrae IPM DANE por departamento (scraping + Excel)
3. extract_api        - Extrae datos geograficos desde API DIVIPOLA
4. transform          - Limpia, homologa y une las tres fuentes
5. validate           - Ejecuta suite de validacion con Great Expectations
6. load               - Carga al Data Warehouse solo si validacion pasa

Dependencias:
    [extract_violencia] --+
    [extract_dane]        +--> [transform] --> [validate] --> [load]
    [extract_api]       --+

Frecuencia: diaria (@daily)
"""

import logging
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

# Agregar el directorio raiz del proyecto al path para importar src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

# --- Argumentos por defecto del DAG ---

DEFAULT_ARGS = {
    "owner": "juan_jose_alban",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

# --- Funciones de las tareas ---


def task_extract_violencia(**kwargs):
    """Tarea 1: Extrae y valida el CSV de violencia intrafamiliar."""
    from src.extract import extract_violencia

    logger.info("DAG - Tarea: extract_violencia")
    df = extract_violencia()
    logger.info("Violencia extraida: %s", str(df.shape))
    kwargs["ti"].xcom_push(key="violencia_shape", value=str(df.shape))
    return str(df.shape)


def task_extract_dane(**kwargs):
    """Tarea 2: Extrae el indice de pobreza multidimensional del DANE."""
    from src.extract import extract_dane_ipm

    logger.info("DAG - Tarea: extract_dane")
    df = extract_dane_ipm()
    logger.info("DANE IPM extraido: %s", str(df.shape))
    kwargs["ti"].xcom_push(key="dane_shape", value=str(df.shape))
    return str(df.shape)


def task_extract_api(**kwargs):
    """Tarea 3: Extrae datos geograficos desde la API publica DIVIPOLA."""
    from src.extract import extract_divipola

    logger.info("DAG - Tarea: extract_api (DIVIPOLA)")
    df = extract_divipola()
    logger.info("DIVIPOLA extraido: %s", str(df.shape))
    kwargs["ti"].xcom_push(key="divipola_shape", value=str(df.shape))
    return str(df.shape)


def task_transform(**kwargs):
    """Tarea 4: Transforma y une las tres fuentes en el dataset final."""
    from src.extract import extract_violencia, extract_dane_ipm, extract_divipola
    from src.transform import run_transform

    logger.info("DAG - Tarea: transform")
    df_violencia = extract_violencia()
    df_dane = extract_dane_ipm()
    df_divipola = extract_divipola()

    df_final = run_transform(df_violencia, df_dane, df_divipola)
    logger.info("Dataset final generado: %s", str(df_final.shape))
    kwargs["ti"].xcom_push(key="final_shape", value=str(df_final.shape))
    return str(df_final.shape)


def task_validate(**kwargs):
    """Tarea 5: Valida el dataset con Great Expectations."""
    from src.validate import run_validate

    logger.info("DAG - Tarea: validate")
    try:
        resultado = run_validate()
        if not resultado:
            raise RuntimeError("Validacion fallo - se detiene la carga.")
        logger.info("Validacion aprobada.")
        kwargs["ti"].xcom_push(key="validation_passed", value=True)
        return True
    except RuntimeError as e:
        logger.error("Validacion rechazada: %s", str(e))
        raise


def task_load(**kwargs):
    """Tarea 6: Carga al Data Warehouse (solo si validacion paso)."""
    from src.load import run_load

    validation_passed = kwargs["ti"].xcom_pull(
        task_ids="validate", key="validation_passed"
    )
    if not validation_passed:
        raise AirflowSkipException("Carga omitida: la validacion no paso.")

    logger.info("DAG - Tarea: load")
    run_load()
    logger.info("Carga al Data Warehouse completada.")
    return "load_complete"


# --- Definicion del DAG ---

with DAG(
    dag_id="etl_violencia_intrafamiliar",
    description=(
        "Pipeline ETL completo: Violencia Intrafamiliar Colombia. "
        "Extrae 3 fuentes, transforma, valida con GE y carga a Supabase."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "violencia", "colombia", "uao", "segunda-entrega"],
) as dag:

    t_extract_violencia = PythonOperator(
        task_id="extract_violencia",
        python_callable=task_extract_violencia,
        doc_md="""
        **Extraccion Fuente 1: Violencia Intrafamiliar**

        Lee el CSV Reporte Delito Violencia Intrafamiliar - Policia Nacional
        desde el archivo local o lo descarga desde datos.gov.co.
        """,
    )

    t_extract_dane = PythonOperator(
        task_id="extract_dane",
        python_callable=task_extract_dane,
        doc_md="""
        **Extraccion Fuente 2: DANE IPM Departamental**

        Descarga el Excel de Pobreza Multidimensional Departamental
        y lo transforma a formato largo (departamento, anio, ipm_total).
        """,
    )

    t_extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=task_extract_api,
        doc_md="""
        **Extraccion Fuente 3: API DIVIPOLA**

        Consulta la API publica del DANE para obtener la division
        politico-administrativa de Colombia con coordenadas geograficas.
        """,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        doc_md="""
        **Transformacion y Merge**

        1. Normaliza departamentos (sin tildes, mayusculas)
        2. Agrega violencia por departamento y anio
        3. Aplica homologacion territorial
        4. Hace merge de las 3 fuentes (511 registros x 10 columnas)
        """,
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=task_validate,
        doc_md="""
        **Validacion con Great Expectations**

        Suite de 13 validaciones: no nulos, rangos de anios,
        valores no negativos, IPM entre 0-100, unicidad cod_dpto+anio.
        Si alguna critica falla, el DAG se detiene.
        """,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        doc_md="""
        **Carga al Data Warehouse (Supabase/PostgreSQL)**

        Star Schema: fact_violencia + dim_departamento + dim_tiempo.
        Solo se ejecuta si validate paso exitosamente.
        Usa UPSERT para evitar duplicados.
        """,
    )

    # Dependencias: 3 extracciones en paralelo, luego transform -> validate -> load
    [t_extract_violencia, t_extract_dane, t_extract_api] >> t_transform >> t_validate >> t_load
