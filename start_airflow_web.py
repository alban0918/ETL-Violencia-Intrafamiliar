"""
start_airflow_web.py - Inicia el webserver de Airflow en Windows usando Waitress.

Uso:
    python start_airflow_web.py

Luego acceder a: http://localhost:8080
Usuario: admin  |  Contrasena: admin123
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# Variables de entorno ANTES de importar Airflow
os.environ["AIRFLOW__CORE__DAGS_FOLDER"]      = str(PROJECT_ROOT / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"]    = "False"
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "WARNING"

# Agregar el proyecto al path para que los DAGs encuentren src/
sys.path.insert(0, str(PROJECT_ROOT))

print("=" * 55)
print("  Airflow Webserver - Windows (via Waitress)")
print("=" * 55)
print(f"  Proyecto:  {PROJECT_ROOT}")
print(f"  DAGs:      {PROJECT_ROOT / 'dags'}")
print("  URL:       http://localhost:8080")
print("  Usuario:   admin")
print("  Password:  admin123")
print("  DAG:       etl_violencia_intrafamiliar")
print("=" * 55)
print("  Sincronizando DAG con la base de datos...")

# Sincronizar el DAG en la tabla 'dag' para que aparezca en la UI
try:
    from airflow.models import DagBag
    from airflow import settings as af_settings

    db = DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)
    if "etl_violencia_intrafamiliar" in db.dags:
        dag_obj = db.dags["etl_violencia_intrafamiliar"]
        session = af_settings.Session()
        dag_obj.sync_to_db(session=session)
        session.commit()
        session.close()
        print("  DAG sincronizado OK")
    else:
        print("  AVISO: DAG no encontrado en DagBag:", list(db.dags.keys()))
        if db.import_errors:
            print("  Errores de importacion:", db.import_errors)
except Exception as e:
    print(f"  AVISO: No se pudo sincronizar el DAG: {e}")

print("=" * 55)
print("  Iniciando... (puede tardar 20-30 segundos)")
print("  Presiona Ctrl+C para detener")
print("=" * 55)

from airflow.www.app import cached_app
app = cached_app()

from waitress import serve
serve(app, host="0.0.0.0", port=8080, threads=4)
