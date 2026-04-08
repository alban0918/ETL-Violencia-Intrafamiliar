"""
transform.py — Transformación, homologación y merge de las tres fuentes.

Pasos:
1. Normalizar nombres de departamentos (unidecode, mayúsculas, sin sufijos "(CT)")
2. Agregar violencia por departamento y año
3. Calcular columnas desagregadas (género, grupo etario)
4. Extraer tabla geográfica departamental desde DIVIPOLA
5. Homologar nombres territoriales
6. Merge final de las tres fuentes
7. Guardar dataset final enriquecido
"""

import logging

import pandas as pd
import re
from unidecode import unidecode

from src.config import (
    DATASET_FINAL_FILE,
    HOMOLOGACION_DEPARTAMENTOS,
    setup_logging,
)

logger = logging.getLogger(__name__)


# ─── Normalización ────────────────────────────────────────────────────────────

def normalizar_texto(x) -> str | None:
    """Normaliza texto: mayúsculas, sin tildes, sin sufijos geográficos."""
    if pd.isna(x):
        return None
    x = str(x).upper().strip()
    x = re.sub(r"\s*\(CT\)", "", x)
    x = re.sub(r"\s+", " ", x)
    x = unidecode(x)
    return x


# ─── Transformaciones por fuente ─────────────────────────────────────────────

def transform_violencia(df: pd.DataFrame) -> dict:
    """
    A partir del DataFrame de violencia crudo, genera las tablas agregadas:
    - violencia_agregada: total_casos por departamento y año
    - femenino, masculino, adultos, menores: desglose por grupo
    """
    logger.info("TRANSFORM — Normalizando y agregando datos de violencia")

    df = df.copy()
    df["departamento_limpio"] = df["departamento"].apply(normalizar_texto)
    df["anio"] = pd.to_numeric(df.get("anio", pd.Series(dtype=float)), errors="coerce")
    df["cantidad"] = pd.to_numeric(df.get("cantidad", pd.Series(dtype=float)), errors="coerce")

    def agregar(filtro_col, filtro_val, col_result):
        subset = df[df[filtro_col] == filtro_val] if filtro_col else df
        return (
            subset
            .groupby(["departamento_limpio", "anio"], as_index=False)
            .agg(**{col_result: ("cantidad", "sum")})
        )

    violencia_agregada = (
        df.groupby(["departamento_limpio", "anio"], as_index=False)
        .agg(total_casos=("cantidad", "sum"))
    )

    femenino = agregar("genero", "FEMENINO", "casos_femenino")
    masculino = agregar("genero", "MASCULINO", "casos_masculino")
    adultos = agregar("grupo_etario", "ADULTOS", "casos_adultos")
    menores = agregar("grupo_etario", "MENORES", "casos_menores")

    logger.info(f"Violencia agregada: {violencia_agregada.shape}")
    return {
        "agregada": violencia_agregada,
        "femenino": femenino,
        "masculino": masculino,
        "adultos": adultos,
        "menores": menores,
    }


def transform_dane(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza el DataFrame del DANE."""
    df = df.copy()
    df["departamento_limpio"] = df["departamento"].apply(normalizar_texto)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
    df["ipm_total"] = pd.to_numeric(df["ipm_total"], errors="coerce")
    return df


def transform_divipola(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae la tabla de departamentos únicos desde DIVIPOLA.
    Normaliza cod_dpto a string de 2 dígitos con zero-padding.
    """
    df = df.copy()
    df["departamento_limpio"] = df["dpto"].apply(normalizar_texto)

    geo_deptos = (
        df[["cod_dpto", "dpto", "departamento_limpio"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # Normalizar cod_dpto a string 2 dígitos
    geo_deptos["cod_dpto"] = pd.to_numeric(geo_deptos["cod_dpto"], errors="coerce")
    geo_deptos["cod_dpto"] = geo_deptos["cod_dpto"].astype("Int64")
    geo_deptos["cod_dpto"] = geo_deptos["cod_dpto"].astype(str).replace("<NA>", pd.NA)
    geo_deptos["cod_dpto"] = geo_deptos["cod_dpto"].apply(
        lambda x: x.zfill(2) if pd.notna(x) else x
    )

    # Añadir San Andrés manualmente (no siempre está en la API con el nombre completo)
    extra = pd.DataFrame({
        "cod_dpto": ["88"],
        "dpto": ["SAN ANDRES Y PROVIDENCIA"],
        "departamento_limpio": ["SAN ANDRES, PROVIDENCIA Y SANTA CATALINA"],
    })
    geo_deptos = pd.concat([geo_deptos, extra], ignore_index=True)
    geo_deptos = geo_deptos.drop_duplicates(subset=["departamento_limpio"], keep="first")
    return geo_deptos.reset_index(drop=True)


# ─── Homologación territorial ─────────────────────────────────────────────────

def aplicar_homologacion(df: pd.DataFrame, col: str = "departamento_limpio") -> pd.DataFrame:
    """Aplica el mapa de homologación de nombres departamentales."""
    df = df.copy()
    df[col] = df[col].replace(HOMOLOGACION_DEPARTAMENTOS)
    return df


# ─── Merge final ─────────────────────────────────────────────────────────────

def merge_final(
    violencia_dict: dict,
    df_dane: pd.DataFrame,
    geo_deptos: pd.DataFrame,
) -> pd.DataFrame:
    """
    Une violencia + DANE IPM + información geográfica DIVIPOLA.
    Retorna el dataset final enriquecido con 10 columnas.
    """
    logger.info("TRANSFORM — Ejecutando merge final de las 3 fuentes")

    # Homologar nombres en todas las tablas
    agg = aplicar_homologacion(violencia_dict["agregada"])
    femenino = aplicar_homologacion(violencia_dict["femenino"])
    masculino = aplicar_homologacion(violencia_dict["masculino"])
    adultos = aplicar_homologacion(violencia_dict["adultos"])
    menores = aplicar_homologacion(violencia_dict["menores"])
    dane = aplicar_homologacion(df_dane)
    geo = aplicar_homologacion(geo_deptos)

    # Merge principal
    df = agg.merge(dane[["departamento_limpio", "anio", "ipm_total"]],
                   on=["departamento_limpio", "anio"], how="left")
    df = df.merge(femenino, on=["departamento_limpio", "anio"], how="left")
    df = df.merge(masculino, on=["departamento_limpio", "anio"], how="left")
    df = df.merge(adultos, on=["departamento_limpio", "anio"], how="left")
    df = df.merge(menores, on=["departamento_limpio", "anio"], how="left")
    df = df.merge(geo, on="departamento_limpio", how="left")

    # Ordenar columnas
    columnas_finales = [
        "cod_dpto", "dpto", "departamento_limpio", "anio",
        "total_casos", "casos_femenino", "casos_masculino",
        "casos_adultos", "casos_menores", "ipm_total",
    ]
    df = df[columnas_finales].copy()

    # Rellenar nulos en conteos con 0
    for col in ["casos_femenino", "casos_masculino", "casos_adultos", "casos_menores"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Completar cod_dpto y dpto faltantes desde el mapa geo
    mapa_cod = geo.set_index("departamento_limpio")["cod_dpto"].to_dict()
    mapa_dpto = geo.set_index("departamento_limpio")["dpto"].to_dict()
    df["cod_dpto"] = df["cod_dpto"].fillna(df["departamento_limpio"].map(mapa_cod))
    df["dpto"] = df["dpto"].fillna(df["departamento_limpio"].map(mapa_dpto))

    # Normalizar tipos finales
    df["cod_dpto"] = (
        pd.to_numeric(df["cod_dpto"], errors="coerce")
        .astype("Int64")
        .astype(str)
        .replace("<NA>", pd.NA)
    )
    df["cod_dpto"] = df["cod_dpto"].apply(lambda x: x.zfill(2) if pd.notna(x) else x)
    df["anio"] = pd.to_numeric(df["anio"], errors="coerce").astype("Int64")
    df["total_casos"] = pd.to_numeric(df["total_casos"], errors="coerce").fillna(0).astype(int)
    df["ipm_total"] = pd.to_numeric(df["ipm_total"], errors="coerce")

    logger.info(f"TRANSFORM — Dataset final: {df.shape}")
    logger.info(f"Nulos por columna:\n{df.isnull().sum()}")
    return df


# ─── Pipeline completo de transformación ─────────────────────────────────────

def run_transform(
    df_violencia: pd.DataFrame,
    df_dane: pd.DataFrame,
    df_divipola: pd.DataFrame,
) -> pd.DataFrame:
    """
    Ejecuta el pipeline completo de transformación y retorna el dataset final.
    Guarda el resultado en data/final/04_dataset_final_enriquecido.csv.
    """
    logger.info("=== TRANSFORM — Inicio del pipeline de transformación ===")

    violencia_dict = transform_violencia(df_violencia)
    df_dane_t = transform_dane(df_dane)
    geo_deptos = transform_divipola(df_divipola)
    df_final = merge_final(violencia_dict, df_dane_t, geo_deptos)

    df_final.to_csv(DATASET_FINAL_FILE, index=False)
    logger.info(f"Dataset final guardado en: {DATASET_FINAL_FILE}")
    logger.info("=== TRANSFORM — Pipeline completado ===")
    return df_final


# ─── Punto de entrada independiente ──────────────────────────────────────────

if __name__ == "__main__":
    from src.extract import extract_violencia, extract_dane_ipm, extract_divipola
    setup_logging("transform")
    logger.info("=== Ejecutando transformación ===")
    df1 = extract_violencia()
    df2 = extract_dane_ipm()
    df3 = extract_divipola()
    df_final = run_transform(df1, df2, df3)
    print(df_final.head())
    print("Nulos:", df_final.isnull().sum().to_dict())
