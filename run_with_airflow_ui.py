# -*- coding: utf-8 -*-
"""
run_with_airflow_ui.py - Ejecuta el pipeline ETL y registra el resultado en Airflow UI.

Uso:
    python run_with_airflow_ui.py
"""

import os
import sys
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

PROJECT_ROOT = Path(__file__).resolve().parent
os.environ["AIRFLOW__CORE__DAGS_FOLDER"]      = str(PROJECT_ROOT / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"]    = "False"
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "WARNING"
sys.path.insert(0, str(PROJECT_ROOT))

import warnings
warnings.filterwarnings("ignore")

AIRFLOW_DB = Path.home() / "airflow" / "airflow.db"
DAG_ID = "etl_violencia_intrafamiliar"
NOW = datetime.now(timezone.utc)
RUN_ID = f"manual__{NOW.strftime('%Y-%m-%dT%H:%M:%S+00:00')}"
NOW_STR = NOW.strftime("%Y-%m-%d %H:%M:%S.%f")

TASKS = [
    "extract_violencia",
    "extract_dane",
    "extract_api",
    "transform",
    "validate",
    "load",
]

print("=" * 60)
print("  ETL Violencia Intrafamiliar - Runner con UI de Airflow")
print("=" * 60)
print(f"  Run ID: {RUN_ID}")
print()


def db_exec(sql, params=()):
    conn = sqlite3.connect(str(AIRFLOW_DB))
    conn.execute(sql, params)
    conn.commit()
    conn.close()


def db_query(sql, params=()):
    conn = sqlite3.connect(str(AIRFLOW_DB))
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return rows


# --- Asegurar que el DAG este en la tabla dag ---
import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from airflow.models import DagBag
    from airflow import settings as af_settings

dagbag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)
if DAG_ID in dagbag.dags:
    sess = af_settings.Session()
    dagbag.dags[DAG_ID].sync_to_db(session=sess)
    sess.commit()
    sess.close()
    print("  DAG sincronizado OK")

# --- Crear DagRun en SQLite directamente ---
existing = db_query("SELECT id FROM dag_run WHERE dag_id=? AND run_id=?", (DAG_ID, RUN_ID))
if not existing:
    db_exec("""
        INSERT INTO dag_run
            (dag_id, run_id, execution_date, start_date, state, run_type,
             external_trigger, clear_number, updated_at)
        VALUES (?, ?, ?, ?, 'running', 'manual', 1, 0, ?)
    """, (DAG_ID, RUN_ID, NOW_STR, NOW_STR, NOW_STR))
    print(f"  DagRun creado: {RUN_ID}")
else:
    db_exec("UPDATE dag_run SET state='running', start_date=? WHERE dag_id=? AND run_id=?",
            (NOW_STR, DAG_ID, RUN_ID))
    print(f"  DagRun existente actualizado")


def set_ti(task_id, state):
    """Inserta o actualiza un TaskInstance en SQLite."""
    t = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    existing = db_query(
        "SELECT task_id FROM task_instance WHERE dag_id=? AND run_id=? AND task_id=? AND map_index=?",
        (DAG_ID, RUN_ID, task_id, -1)
    )
    if not existing:
        db_exec("""
            INSERT INTO task_instance
                (task_id, dag_id, run_id, map_index, state, try_number, max_tries,
                 pool, pool_slots, start_date, end_date, updated_at, operator)
            VALUES (?, ?, ?, -1, ?, 1, 0, 'default_pool', 1, ?, ?, ?, 'PythonOperator')
        """, (task_id, DAG_ID, RUN_ID, state, t, t, t))
    else:
        db_exec("""
            UPDATE task_instance SET state=?, end_date=?, updated_at=?
            WHERE dag_id=? AND run_id=? AND task_id=? AND map_index=-1
        """, (state, t, t, DAG_ID, RUN_ID, task_id))


def run_task(task_id, fn, *args, **kwargs):
    print(f"\n  [{task_id}] Iniciando...")
    set_ti(task_id, "running")
    try:
        result = fn(*args, **kwargs)
        set_ti(task_id, "success")
        print(f"  [{task_id}] OK")
        return result
    except Exception as e:
        set_ti(task_id, "failed")
        print(f"  [{task_id}] ERROR: {e}")
        raise


# ============================================================
# EJECUTAR EL PIPELINE
# ============================================================
print("\n  Ejecutando pipeline...\n")

try:
    from src.extract import extract_violencia, extract_dane_ipm, extract_divipola
    from src.transform import run_transform
    from src.validate import run_validate
    from src.load import run_load

    df_violencia = run_task("extract_violencia", extract_violencia)
    print(f"    -> {df_violencia.shape[0]:,} filas extraidas")

    df_dane = run_task("extract_dane", extract_dane_ipm)
    print(f"    -> {df_dane.shape[0]:,} filas extraidas")

    df_divipola = run_task("extract_api", extract_divipola)
    print(f"    -> {df_divipola.shape[0]:,} filas extraidas")

    df_final = run_task("transform", run_transform, df_violencia, df_dane, df_divipola)
    print(f"    -> Dataset final: {df_final.shape}")

    run_task("validate", run_validate)

    run_task("load", run_load)

    # Marcar DagRun como success
    end_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    db_exec("UPDATE dag_run SET state='success', end_date=?, updated_at=? WHERE dag_id=? AND run_id=?",
            (end_str, end_str, DAG_ID, RUN_ID))

    print("\n" + "=" * 60)
    print("  PIPELINE COMPLETADO - Estado: SUCCESS")
    print("  Recarga la UI: http://localhost:8080")
    print("=" * 60)

except Exception as e:
    end_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    db_exec("UPDATE dag_run SET state='failed', end_date=?, updated_at=? WHERE dag_id=? AND run_id=?",
            (end_str, end_str, DAG_ID, RUN_ID))
    print(f"\n  PIPELINE FALLIDO: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
