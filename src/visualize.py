"""
visualize.py — Visualizaciones desde el Data Warehouse (Supabase/PostgreSQL).

Genera 6 gráficas desde los datos validados y cargados al DW.
Los PNG se guardan en visualizations/ para incluir en el informe.

Ejecución:
    python src/visualize.py
"""

import logging
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sqlalchemy import create_engine

from src.config import BASE_DIR, get_db_connection_string, setup_logging

logger = logging.getLogger(__name__)

VIZ_DIR = BASE_DIR / "visualizations"
VIZ_DIR.mkdir(exist_ok=True)

# Paleta corporativa (oscura, acorde al dashboard de Power BI)
COLOR_PRINCIPAL = "#C9963A"
COLOR_SECUNDARIO = "#4A90D9"
COLOR_FONDO = "#1A1A2E"
COLOR_TEXTO = "#E0E0E0"

plt.rcParams.update({
    "figure.facecolor": COLOR_FONDO,
    "axes.facecolor": "#16213E",
    "axes.edgecolor": "#444",
    "axes.labelcolor": COLOR_TEXTO,
    "xtick.color": COLOR_TEXTO,
    "ytick.color": COLOR_TEXTO,
    "text.color": COLOR_TEXTO,
    "grid.color": "#333355",
    "grid.linewidth": 0.5,
    "font.family": "DejaVu Sans",
})


def _cargar_datos(engine) -> pd.DataFrame:
    """Carga el dataset desde el DW uniendo dimensiones y hechos."""
    query = """
    SELECT
        d.cod_dpto,
        d.dpto,
        d.departamento_limpio,
        t.anio,
        f.total_casos,
        f.casos_femenino,
        f.casos_masculino,
        f.casos_adultos,
        f.casos_menores,
        f.ipm_total
    FROM fact_violencia f
    JOIN dim_departamento d ON f.id_departamento = d.id_departamento
    JOIN dim_tiempo t ON f.id_tiempo = t.id_tiempo
    ORDER BY t.anio, d.dpto
    """
    logger.info("Cargando datos desde el Data Warehouse...")
    from sqlalchemy import text as sqla_text
    with engine.connect() as conn:
        result = conn.execute(sqla_text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    logger.info("Datos cargados: %s", str(df.shape))
    return df


def viz_evolucion_anual(df: pd.DataFrame):
    """Gráfica 1: Evolución anual de casos 2010–2025."""
    casos_anio = df.groupby("anio")["total_casos"].sum().reset_index()

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(casos_anio["anio"], casos_anio["total_casos"],
            color=COLOR_PRINCIPAL, linewidth=2.5, marker="o", markersize=5)
    ax.fill_between(casos_anio["anio"], casos_anio["total_casos"],
                    alpha=0.15, color=COLOR_PRINCIPAL)

    ax.set_title("Evolución Anual de Violencia Intrafamiliar en Colombia",
                 fontsize=14, pad=15, color=COLOR_TEXTO)
    ax.set_xlabel("Año", fontsize=11)
    ax.set_ylabel("Total de Casos", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.set_xticks(casos_anio["anio"])
    ax.tick_params(axis="x", rotation=45)
    ax.grid(True, axis="y")

    fig.tight_layout()
    path = VIZ_DIR / "01_evolucion_anual.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Guardada: %s", path.name)


def viz_top10_departamentos(df: pd.DataFrame):
    """Gráfica 2: Top 10 departamentos con más casos (total histórico)."""
    top10 = (
        df.groupby("dpto")["total_casos"].sum()
        .sort_values(ascending=True)
        .tail(10)
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(top10["dpto"], top10["total_casos"], color=COLOR_PRINCIPAL, alpha=0.85)

    for bar in bars:
        w = bar.get_width()
        ax.text(w + w * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{int(w):,}", va="center", fontsize=9, color=COLOR_TEXTO)

    ax.set_title("Top 10 Departamentos — Total Casos de Violencia Intrafamiliar",
                 fontsize=13, pad=15)
    ax.set_xlabel("Total de Casos Acumulados")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(True, axis="x")

    fig.tight_layout()
    path = VIZ_DIR / "02_top10_departamentos.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Guardada: %s", path.name)


def viz_distribucion_genero(df: pd.DataFrame):
    """Gráfica 3: Distribución por género (femenino vs masculino)."""
    total_f = df["casos_femenino"].sum()
    total_m = df["casos_masculino"].sum()
    total = total_f + total_m

    fig, ax = plt.subplots(figsize=(7, 7))
    sizes = [total_f, total_m]
    labels = [f"FEMENINO\n{total_f/total*100:.1f}%", f"MASCULINO\n{total_m/total*100:.1f}%"]
    colors = [COLOR_PRINCIPAL, COLOR_SECUNDARIO]
    explode = (0.05, 0)

    wedges, texts = ax.pie(sizes, labels=labels, colors=colors, explode=explode,
                           startangle=90, wedgeprops={"width": 0.55, "edgecolor": COLOR_FONDO})
    for t in texts:
        t.set_fontsize(12)
        t.set_color(COLOR_TEXTO)

    ax.set_title("Distribución por Género — Víctimas de Violencia Intrafamiliar",
                 fontsize=13, pad=20)
    centre_circle = plt.Circle((0, 0), 0.45, fc=COLOR_FONDO)
    ax.add_patch(centre_circle)
    ax.text(0, 0, f"{total:,.0f}\nvíctimas", ha="center", va="center",
            fontsize=10, color=COLOR_TEXTO)

    fig.tight_layout()
    path = VIZ_DIR / "03_distribucion_genero.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Guardada: %s", path.name)


def viz_distribucion_etaria(df: pd.DataFrame):
    """Gráfica 4: Distribución por grupo etario (adultos vs menores)."""
    total_a = df["casos_adultos"].sum()
    total_m = df["casos_menores"].sum()
    total = total_a + total_m

    fig, ax = plt.subplots(figsize=(7, 7))
    sizes = [total_a, total_m]
    labels = [f"ADULTOS\n{total_a/total*100:.1f}%", f"MENORES\n{total_m/total*100:.1f}%"]
    colors = [COLOR_PRINCIPAL, "#E05A5A"]

    wedges, texts = ax.pie(sizes, labels=labels, colors=colors, startangle=90,
                           wedgeprops={"width": 0.55, "edgecolor": COLOR_FONDO})
    for t in texts:
        t.set_fontsize(12)
        t.set_color(COLOR_TEXTO)

    ax.set_title("Distribución por Grupo Etario — Víctimas de Violencia Intrafamiliar",
                 fontsize=13, pad=20)
    centre_circle = plt.Circle((0, 0), 0.45, fc=COLOR_FONDO)
    ax.add_patch(centre_circle)

    fig.tight_layout()
    path = VIZ_DIR / "04_distribucion_etaria.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Guardada: %s", path.name)


def viz_ipm_vs_casos(df: pd.DataFrame):
    """Gráfica 5: Correlación IPM vs total de casos (scatter por departamento)."""
    df_scatter = df.dropna(subset=["ipm_total"]).copy()
    df_agg = df_scatter.groupby("dpto").agg(
        ipm_promedio=("ipm_total", "mean"),
        total_casos=("total_casos", "sum")
    ).reset_index()

    fig, ax = plt.subplots(figsize=(10, 6))

    scatter = ax.scatter(
        df_agg["ipm_promedio"], df_agg["total_casos"],
        c=df_agg["total_casos"], cmap="YlOrRd",
        s=80, alpha=0.85, edgecolors="#555", linewidths=0.5
    )

    # Etiquetas para los más destacados
    top_casos = df_agg.nlargest(5, "total_casos")
    top_ipm = df_agg.nlargest(3, "ipm_promedio")
    destacados = pd.concat([top_casos, top_ipm]).drop_duplicates()
    for _, row in destacados.iterrows():
        ax.annotate(row["dpto"], (row["ipm_promedio"], row["total_casos"]),
                    textcoords="offset points", xytext=(5, 5),
                    fontsize=7.5, color=COLOR_TEXTO)

    # Línea de tendencia
    import numpy as np
    z = np.polyfit(df_agg["ipm_promedio"], df_agg["total_casos"], 1)
    p = np.poly1d(z)
    x_line = sorted(df_agg["ipm_promedio"])
    ax.plot(x_line, p(x_line), color=COLOR_PRINCIPAL, linestyle="--", alpha=0.6, linewidth=1.5)

    ax.set_title("Pobreza Multidimensional (IPM) vs Casos de Violencia Intrafamiliar",
                 fontsize=13, pad=15)
    ax.set_xlabel("IPM Promedio por Departamento (%)", fontsize=11)
    ax.set_ylabel("Total de Casos Acumulados", fontsize=11)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax.grid(True)
    plt.colorbar(scatter, ax=ax, label="Total Casos")

    fig.tight_layout()
    path = VIZ_DIR / "05_ipm_vs_casos.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Guardada: %s", path.name)


def viz_heatmap_depto_anio(df: pd.DataFrame):
    """Gráfica 6: Heatmap de casos por departamento y año (top 15 deptos)."""
    top15 = (
        df.groupby("dpto")["total_casos"].sum()
        .sort_values(ascending=False)
        .head(15)
        .index.tolist()
    )
    df_top = df[df["dpto"].isin(top15)]

    pivot = df_top.pivot_table(
        index="dpto", columns="anio", values="total_casos", aggfunc="sum"
    ).fillna(0)

    fig, ax = plt.subplots(figsize=(14, 7))
    sns.heatmap(
        pivot, ax=ax, cmap="YlOrRd", linewidths=0.3, linecolor="#1A1A2E",
        fmt=".0f", annot=True, annot_kws={"size": 7.5},
        cbar_kws={"label": "Total Casos"},
    )
    ax.set_title("Casos de Violencia Intrafamiliar por Departamento y Año — Top 15",
                 fontsize=13, pad=15)
    ax.set_xlabel("Año", fontsize=11)
    ax.set_ylabel("Departamento", fontsize=11)
    ax.tick_params(axis="x", rotation=45)

    fig.tight_layout()
    path = VIZ_DIR / "06_heatmap_depto_anio.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Guardada: %s", path.name)


def run_visualize():
    """Ejecuta todas las visualizaciones desde el DW."""
    logger.info("=== VISUALIZE — Generando gráficas desde el Data Warehouse ===")
    engine = create_engine(get_db_connection_string())
    df = _cargar_datos(engine)

    viz_evolucion_anual(df)
    viz_top10_departamentos(df)
    viz_distribucion_genero(df)
    viz_distribucion_etaria(df)
    viz_ipm_vs_casos(df)
    viz_heatmap_depto_anio(df)

    logger.info("=== VISUALIZE — 6 gráficas guardadas en visualizations/ ===")
    return VIZ_DIR


if __name__ == "__main__":
    setup_logging("visualize")
    run_visualize()
    print(f"\n6 visualizaciones guardadas en: {VIZ_DIR}")
