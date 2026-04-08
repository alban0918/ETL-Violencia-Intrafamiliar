"""
test_pipeline.py — Pruebas básicas del pipeline ETL.

Verifica que:
- Los archivos de datos existen
- Las funciones de transformación producen el shape esperado
- Las validaciones de Great Expectations pasan
- Los módulos importan sin error
"""

import sys
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def test_archivos_raw_existen():
    """Los tres archivos de datos crudos deben existir."""
    from src.config import VIOLENCIA_RAW_FILE, DANE_IPM_FILE, DIVIPOLA_FILE
    assert VIOLENCIA_RAW_FILE.exists(), f"No existe: {VIOLENCIA_RAW_FILE}"
    assert DANE_IPM_FILE.exists(), f"No existe: {DANE_IPM_FILE}"
    assert DIVIPOLA_FILE.exists(), f"No existe: {DIVIPOLA_FILE}"
    print("PASS test_archivos_raw_existen")


def test_dataset_final_existe():
    """El dataset final enriquecido debe existir."""
    from src.config import DATASET_FINAL_FILE
    assert DATASET_FINAL_FILE.exists(), f"No existe: {DATASET_FINAL_FILE}"
    print("PASS test_dataset_final_existe")


def test_dataset_final_columnas():
    """El dataset final debe tener exactamente 10 columnas específicas."""
    from src.config import DATASET_FINAL_FILE
    df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})
    columnas_esperadas = {
        "cod_dpto", "dpto", "departamento_limpio", "anio",
        "total_casos", "casos_femenino", "casos_masculino",
        "casos_adultos", "casos_menores", "ipm_total",
    }
    assert set(df.columns) == columnas_esperadas, (
        f"Columnas inesperadas: {set(df.columns) - columnas_esperadas} | "
        f"Faltantes: {columnas_esperadas - set(df.columns)}"
    )
    print(f"PASS test_dataset_final_columnas ({df.shape})")


def test_dataset_final_sin_nulos_criticos():
    """Las columnas críticas no deben tener nulos."""
    from src.config import DATASET_FINAL_FILE
    df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})
    criticos = ["cod_dpto", "dpto", "departamento_limpio", "anio", "total_casos"]
    for col in criticos:
        nulos = df[col].isnull().sum()
        assert nulos == 0, f"Columna '{col}' tiene {nulos} nulos"
    print("PASS test_dataset_final_sin_nulos_criticos")


def test_dataset_final_unicidad():
    """La combinación cod_dpto + anio debe ser única."""
    from src.config import DATASET_FINAL_FILE
    df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})
    duplicados = df.duplicated(subset=["cod_dpto", "anio"]).sum()
    assert duplicados == 0, f"Hay {duplicados} filas duplicadas en cod_dpto + anio"
    print("PASS test_dataset_final_unicidad")


def test_dataset_final_anios():
    """Los años deben estar en el rango 2010–2025."""
    from src.config import DATASET_FINAL_FILE
    df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})
    anios = pd.to_numeric(df["anio"], errors="coerce")
    assert anios.min() >= 2010, f"Año mínimo inesperado: {anios.min()}"
    assert anios.max() <= 2025, f"Año máximo inesperado: {anios.max()}"
    print(f"PASS test_dataset_final_anios (rango: {int(anios.min())}–{int(anios.max())})")


def test_dataset_final_conteos_positivos():
    """Los conteos de casos no deben ser negativos."""
    from src.config import DATASET_FINAL_FILE
    df = pd.read_csv(DATASET_FINAL_FILE, dtype={"cod_dpto": str})
    for col in ["total_casos", "casos_femenino", "casos_masculino", "casos_adultos", "casos_menores"]:
        vals = pd.to_numeric(df[col], errors="coerce")
        assert (vals >= 0).all(), f"Valores negativos en '{col}'"
    print("PASS test_dataset_final_conteos_positivos")


def test_great_expectations_pasa():
    """La suite de Great Expectations debe pasar sin errores críticos."""
    from src.validate import run_validate
    resultado = run_validate()
    assert resultado is True, "Great Expectations retornó False"
    print("PASS test_great_expectations_pasa")


def test_transformacion_basica():
    """La función de transformación produce el shape esperado."""
    from src.extract import extract_violencia, extract_dane_ipm, extract_divipola
    from src.transform import run_transform

    df_v = extract_violencia()
    df_d = extract_dane_ipm()
    df_g = extract_divipola()

    df_final = run_transform(df_v, df_d, df_g)

    assert df_final.shape[1] == 10, f"Se esperaban 10 columnas, hay {df_final.shape[1]}"
    assert df_final.shape[0] > 100, f"Muy pocos registros: {df_final.shape[0]}"
    print(f"PASS test_transformacion_basica (shape: {df_final.shape})")


if __name__ == "__main__":
    print("\n=== Ejecutando pruebas del pipeline ETL ===\n")
    pruebas = [
        test_archivos_raw_existen,
        test_dataset_final_existe,
        test_dataset_final_columnas,
        test_dataset_final_sin_nulos_criticos,
        test_dataset_final_unicidad,
        test_dataset_final_anios,
        test_dataset_final_conteos_positivos,
        test_great_expectations_pasa,
        test_transformacion_basica,
    ]

    pasaron = 0
    fallaron = []
    for prueba in pruebas:
        try:
            prueba()
            pasaron += 1
        except Exception as e:
            fallaron.append((prueba.__name__, str(e)))
            print(f"FAIL {prueba.__name__}: {e}")

    print(f"\n{'='*50}")
    print(f"Resultado: {pasaron}/{len(pruebas)} pruebas pasaron")
    if fallaron:
        print("Fallaron:")
        for nombre, err in fallaron:
            print(f"  - {nombre}: {err}")
    else:
        print("Todas las pruebas pasaron.")
    print("=" * 50)
