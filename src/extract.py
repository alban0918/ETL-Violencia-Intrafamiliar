"""
extract.py — Extracción de las tres fuentes de datos.

Fuente 1: CSV Violencia Intrafamiliar (datos.gov.co o archivo local)
Fuente 2: DANE IPM Departamental (web scraping + Excel)
Fuente 3: API DIVIPOLA (API pública datos.gov.co)
"""

import logging
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from unidecode import unidecode

from src.config import (
    VIOLENCIA_RAW_FILE,
    DANE_IPM_FILE,
    DIVIPOLA_FILE,
    DANE_EXCEL_FILE,
    DANE_URL,
    DANE_SHEET,
    DIVIPOLA_API_URL,
    DIVIPOLA_LIMIT,
    setup_logging,
)

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0"}


# ─── Utilidades ────────────────────────────────────────────────────────────────

def limpiar_columnas(cols: list) -> list:
    """Normaliza nombres de columnas: lowercase, sin tildes, guiones → underscore."""
    resultado = []
    for c in cols:
        c = str(c)
        c = unidecode(c)
        c = c.strip().lower()
        c = re.sub(r"[\s\-\.\/]+", "_", c)
        resultado.append(c)
    return resultado


# ─── Fuente 1: Violencia Intrafamiliar ────────────────────────────────────────

def extract_violencia() -> pd.DataFrame:
    """
    Carga el CSV de violencia intrafamiliar desde el archivo local procesado.
    Si no existe, intenta descargarlo desde datos.gov.co.
    Retorna DataFrame limpio con las columnas necesarias.
    """
    logger.info("FUENTE 1 — Iniciando extracción: Violencia Intrafamiliar")

    if VIOLENCIA_RAW_FILE.exists():
        logger.info(f"Leyendo archivo local: {VIOLENCIA_RAW_FILE}")
        df = pd.read_csv(VIOLENCIA_RAW_FILE)
    else:
        logger.warning("Archivo local no encontrado. Descargando desde datos.gov.co...")
        from src.config import VIOLENCIA_CSV_URL
        resp = requests.get(VIOLENCIA_CSV_URL, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        from io import StringIO
        df = pd.read_csv(StringIO(resp.text))
        df.columns = limpiar_columnas(df.columns)
        df.to_csv(VIOLENCIA_RAW_FILE, index=False)
        logger.info(f"Archivo guardado en: {VIOLENCIA_RAW_FILE}")

    df.columns = limpiar_columnas(df.columns)

    # Transformación básica de tipos
    for col in ["departamento", "municipio", "genero", "grupo_etario"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()

    if "fecha_hecho" in df.columns:
        df["fecha_hecho"] = pd.to_datetime(df["fecha_hecho"], errors="coerce", format="mixed")
        df["anio"] = df["fecha_hecho"].dt.year

    if "cantidad" in df.columns:
        df["cantidad"] = pd.to_numeric(df["cantidad"], errors="coerce")

    logger.info(f"FUENTE 1 — OK. Dimensiones: {df.shape}")
    return df


# ─── Fuente 2: DANE IPM ───────────────────────────────────────────────────────

def extract_dane_ipm() -> pd.DataFrame:
    """
    Carga datos del DANE sobre pobreza multidimensional departamental.
    Primero busca el CSV procesado local; si no existe, hace scraping + parseo del Excel.
    Retorna DataFrame con columnas: departamento, anio, ipm_total.
    """
    logger.info("FUENTE 2 — Iniciando extracción: DANE IPM Departamental")

    if DANE_IPM_FILE.exists():
        logger.info(f"Leyendo archivo local: {DANE_IPM_FILE}")
        df = pd.read_csv(DANE_IPM_FILE)
        df["departamento"] = df["departamento"].astype(str).str.upper().str.strip()
        df["anio"] = pd.to_numeric(df["anio"], errors="coerce")
        df["ipm_total"] = pd.to_numeric(df["ipm_total"], errors="coerce")
        logger.info(f"FUENTE 2 — OK. Dimensiones: {df.shape}")
        return df

    # Scraping si no existe el CSV local
    logger.info("Archivo local no encontrado. Descargando desde DANE...")
    response = requests.get(DANE_URL, headers=HEADERS, timeout=60)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")

    excel_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        full_url = urljoin(DANE_URL, href)
        if full_url.endswith(".xls") or full_url.endswith(".xlsx"):
            excel_links.append(full_url)

    logger.info(f"Archivos Excel encontrados en DANE: {len(excel_links)}")

    link_ipm = None
    for link in excel_links:
        lower = link.lower()
        if "departamental" in lower and ("pmultidimensional" in lower or "pobreza" in lower):
            link_ipm = link
            break

    if not link_ipm and excel_links:
        link_ipm = excel_links[0]
        logger.warning(f"No se encontró archivo departamental específico. Usando: {link_ipm}")

    if not link_ipm:
        raise RuntimeError("No se encontró ningún archivo Excel en la página del DANE.")

    logger.info(f"Descargando: {link_ipm}")
    r = requests.get(link_ipm, headers=HEADERS, timeout=60)
    r.raise_for_status()
    with open(DANE_EXCEL_FILE, "wb") as f:
        f.write(r.content)

    df = _parsear_excel_dane(DANE_EXCEL_FILE)
    df.to_csv(DANE_IPM_FILE, index=False)
    logger.info(f"Archivo guardado en: {DANE_IPM_FILE}")
    logger.info(f"FUENTE 2 — OK. Dimensiones: {df.shape}")
    return df


def _parsear_excel_dane(filepath) -> pd.DataFrame:
    """Parsea el Excel del DANE (estructura tipo informe) a DataFrame plano."""
    df_raw = pd.read_excel(filepath, sheet_name=DANE_SHEET, header=None)
    logger.info(f"Excel crudo: {df_raw.shape}")

    fila_anios = df_raw.iloc[11].tolist()
    fila_sub = df_raw.iloc[12].tolist()

    columnas = []
    for i in range(len(df_raw.columns)):
        if i == 0:
            columnas.append("departamento")
        else:
            anio_str = str(fila_anios[i]).replace("**", "").strip() if pd.notna(fila_anios[i]) else ""
            sub_str = str(fila_sub[i]).strip() if pd.notna(fila_sub[i]) else ""
            columnas.append(f"{anio_str}_{sub_str}")

    df_dane = df_raw.iloc[13:].copy()
    df_dane.columns = columnas
    df_dane = df_dane.reset_index(drop=True)

    # Filtrar solo columnas "Total" de cada año
    cols_utiles = ["departamento"] + [c for c in df_dane.columns if "Total" in str(c)]
    df_dane = df_dane[cols_utiles]

    rename_dict = {}
    for col in df_dane.columns:
        if col != "departamento":
            anio = str(col).replace("_Total", "").replace("*", "").strip()
            rename_dict[col] = anio
    df_dane = df_dane.rename(columns=rename_dict)

    df_long = df_dane.melt(id_vars="departamento", var_name="anio", value_name="ipm_total")
    df_long["departamento"] = df_long["departamento"].astype(str).str.upper().str.strip()
    df_long["anio"] = pd.to_numeric(df_long["anio"], errors="coerce")
    df_long["ipm_total"] = pd.to_numeric(df_long["ipm_total"], errors="coerce")
    df_long = df_long.dropna().reset_index(drop=True)
    return df_long


# ─── Fuente 3: API DIVIPOLA ───────────────────────────────────────────────────

def extract_divipola() -> pd.DataFrame:
    """
    Consulta la API pública DIVIPOLA (datos.gov.co) para obtener
    información geográfica de municipios colombianos.
    Retorna DataFrame con: cod_dpto, dpto, cod_mpio, nom_mpio, tipo_municipio, longitud, latitud.
    """
    logger.info("FUENTE 3 — Iniciando extracción: API DIVIPOLA")

    if DIVIPOLA_FILE.exists():
        logger.info(f"Leyendo archivo local: {DIVIPOLA_FILE}")
        df = pd.read_csv(DIVIPOLA_FILE)
        logger.info(f"FUENTE 3 — OK. Dimensiones: {df.shape}")
        return df

    url = f"{DIVIPOLA_API_URL}?$limit={DIVIPOLA_LIMIT}"
    logger.info(f"Consultando API: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    df = pd.DataFrame(resp.json())
    df.columns = limpiar_columnas(df.columns)

    columnas_requeridas = ["cod_dpto", "dpto", "cod_mpio", "nom_mpio", "tipo_municipio", "longitud", "latitud"]
    faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"La API DIVIPOLA no retornó las columnas esperadas: {faltantes}")

    df = df[columnas_requeridas].copy()
    df["dpto"] = df["dpto"].astype(str).str.upper().str.strip()
    df["nom_mpio"] = df["nom_mpio"].astype(str).str.upper().str.strip()
    df["tipo_municipio"] = df["tipo_municipio"].astype(str).str.upper().str.strip()

    for coord in ["longitud", "latitud"]:
        df[coord] = df[coord].astype(str).str.replace(",", ".", regex=False)
        df[coord] = pd.to_numeric(df[coord], errors="coerce")

    df = df.drop_duplicates().reset_index(drop=True)
    df.to_csv(DIVIPOLA_FILE, index=False)
    logger.info(f"Archivo guardado en: {DIVIPOLA_FILE}")
    logger.info(f"FUENTE 3 — OK. Dimensiones: {df.shape}")
    return df


# ─── Punto de entrada independiente ──────────────────────────────────────────

if __name__ == "__main__":
    setup_logging("extract")
    logger.info("=== Ejecutando extracción de las 3 fuentes ===")
    df1 = extract_violencia()
    df2 = extract_dane_ipm()
    df3 = extract_divipola()
    logger.info(f"Violencia: {df1.shape} | DANE: {df2.shape} | DIVIPOLA: {df3.shape}")
    logger.info("=== Extracción completa ===")
