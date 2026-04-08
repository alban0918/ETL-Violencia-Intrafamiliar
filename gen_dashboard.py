# -*- coding: utf-8 -*-
"""Genera el dashboard ejecutivo final."""
import os, warnings
warnings.filterwarnings("ignore")
from dotenv import load_dotenv; load_dotenv()
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches

engine = create_engine(
    f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
)
with engine.connect() as conn:
    df = pd.DataFrame(
        conn.execute(text("""
            SELECT d.cod_dpto, d.dpto, t.anio,
                   f.total_casos, f.casos_femenino, f.casos_masculino,
                   f.casos_adultos, f.casos_menores, f.ipm_total
            FROM fact_violencia f
            JOIN dim_departamento d ON f.id_departamento = d.id_departamento
            JOIN dim_tiempo      t ON f.id_tiempo        = t.id_tiempo
            ORDER BY t.anio, d.dpto
        """)).fetchall(),
        columns=["cod_dpto","dpto","anio","total_casos","casos_femenino",
                 "casos_masculino","casos_adultos","casos_menores","ipm_total"]
    )

# ── Paleta ──────────────────────────────────────────────────────────────────
BG     = "#0F1923"
PANEL  = "#162032"
A1     = "#E8A838"   # dorado
A2     = "#3A9BD5"   # azul
A3     = "#E05A6A"   # rojo
A4     = "#4CAF7D"   # verde
TEXT   = "#E8EDF2"
SUB    = "#8FA3B1"
GRID   = "#1E2E3E"
BORDER = "#243447"

# ── Metricas ────────────────────────────────────────────────────────────────
total   = int(df.total_casos.sum())
fem     = int(df.casos_femenino.sum())
masc    = int(df.casos_masculino.sum())
adultos = int(df.casos_adultos.sum())
menores = int(df.casos_menores.sum())
pct_fem = fem / (fem + masc) * 100
anual   = df.groupby("anio")["total_casos"].sum()
peak_yr = int(anual.idxmax())
peak_v  = int(anual.max())
top10   = df.groupby("dpto")["total_casos"].sum().sort_values().tail(10)
scatter = df.dropna(subset=["ipm_total"]).groupby("dpto").agg(
    ipm=("ipm_total","mean"), casos=("total_casos","sum")
).reset_index()

# ── Figura ───────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(22, 13.5), facecolor=BG)
gs  = gridspec.GridSpec(
    3, 4, figure=fig,
    height_ratios=[0.65, 1.45, 1.45],
    hspace=0.58, wspace=0.38,
    left=0.04, right=0.97, top=0.88, bottom=0.06
)

# Header
fig.text(0.5, 0.948, "VIOLENCIA INTRAFAMILIAR EN COLOMBIA",
         ha="center", fontsize=22, fontweight="bold", color=TEXT)
fig.text(0.5, 0.915,
         u"An\u00e1lisis ETL \u2014 Datos Polic\u00eda Nacional | DANE IPM | DIVIPOLA"
         u"  \u00b7  Per\u00edodo 2010\u20132025  \u00b7  32 Departamentos",
         ha="center", fontsize=11, color=SUB)
fig.add_artist(plt.Line2D([0.04,0.97],[0.904,0.904],
               transform=fig.transFigure, color=A1, linewidth=1.5, alpha=0.6))

# ── FILA 0: KPI cards ───────────────────────────────────────────────────────
kpis = [
    (f"{total:,}",    "CASOS TOTALES\n2010\u20132025",  A1, f"16 \u00e1os de registro"),
    (f"{pct_fem:.1f}%", u"V\u00cdCTIMAS\nFEMENINAS",   A3, f"{fem:,} casos"),
    (str(peak_yr),    "A\u00d1O PICO",                  A2, f"{peak_v:,} casos"),
    ("32",            "DEPARTAMENTOS",                   A4, "511 registros en DW"),
]
for col, (val, lbl, col_c, sub) in enumerate(kpis):
    ax = fig.add_subplot(gs[0, col])
    ax.set_facecolor(PANEL); ax.set_xticks([]); ax.set_yticks([])
    for sp in ax.spines.values(): sp.set_edgecolor(col_c); sp.set_linewidth(2)
    ax.text(0.5, 0.70, val,  ha="center", va="center", transform=ax.transAxes,
            fontsize=26, fontweight="bold", color=col_c)
    ax.text(0.5, 0.37, lbl,  ha="center", va="center", transform=ax.transAxes,
            fontsize=9.5, color=TEXT, linespacing=1.5)
    ax.text(0.5, 0.10, sub,  ha="center", va="center", transform=ax.transAxes,
            fontsize=8, color=SUB)

# ── FILA 1 izq: Evolucion anual ─────────────────────────────────────────────
ax1 = fig.add_subplot(gs[1, 0:2])
ax1.set_facecolor(PANEL)
for sp in ["top","right"]: ax1.spines[sp].set_visible(False)
for sp in ["bottom","left"]: ax1.spines[sp].set_color(BORDER)

yrs = anual.index.tolist(); vls = anual.values.tolist()
ax1.fill_between(yrs, vls, alpha=0.18, color=A1)
ax1.plot(yrs, vls, color=A1, lw=2.5, marker="o", ms=5, zorder=3)
ax1.annotate(f"Pico {peak_yr}: {peak_v:,}",
             xy=(peak_yr, peak_v), xytext=(peak_yr-2, peak_v*0.90),
             color=A1, fontsize=8.5,
             arrowprops=dict(arrowstyle="->", color=A1, lw=1.2))
ax1.set_title("Evolucion Anual de Casos", fontsize=12, fontweight="bold",
              color=TEXT, pad=10, loc="left")
ax1.set_xlabel("Año", color=SUB, fontsize=9)
ax1.set_ylabel("Total de Casos", color=SUB, fontsize=9)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax1.set_xticks(yrs); ax1.tick_params(axis="x", rotation=45, colors=SUB, labelsize=8)
ax1.tick_params(axis="y", colors=SUB, labelsize=8)
ax1.grid(True, axis="y", color=GRID, lw=0.7)

# ── FILA 1 der: Top 10 departamentos ────────────────────────────────────────
ax2 = fig.add_subplot(gs[1, 2:4])
ax2.set_facecolor(PANEL)
for sp in ["top","right"]: ax2.spines[sp].set_visible(False)
for sp in ["bottom","left"]: ax2.spines[sp].set_color(BORDER)

bar_colors = [A1 if i == len(top10)-1 else A2 for i in range(len(top10))]
bars = ax2.barh(top10.index, top10.values, color=bar_colors, alpha=0.88, height=0.65)
for bar in bars:
    w = bar.get_width()
    ax2.text(w + w*0.015, bar.get_y() + bar.get_height()/2,
             f"{int(w):,}", va="center", fontsize=7.5, color=TEXT)
ax2.set_title("Top 10 Departamentos — Casos Acumulados", fontsize=12,
              fontweight="bold", color=TEXT, pad=10, loc="left")
ax2.set_xlabel("Total Casos", color=SUB, fontsize=9)
ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax2.tick_params(axis="x", colors=SUB, labelsize=7.5)
ax2.tick_params(axis="y", colors=TEXT, labelsize=8.5)
ax2.grid(True, axis="x", color=GRID, lw=0.7)

# ── FILA 2 col 0: Donut genero ───────────────────────────────────────────────
ax3 = fig.add_subplot(gs[2, 0])
ax3.set_facecolor(PANEL)
for sp in ax3.spines.values(): sp.set_visible(False)
ax3.pie([fem, masc], colors=[A3, A2], startangle=90,
        wedgeprops={"width":0.52,"edgecolor":BG,"linewidth":2},
        explode=(0.03,0))
ax3.text(0, 0.10, f"{pct_fem:.1f}%", ha="center", va="center",
         fontsize=19, fontweight="bold", color=A3)
ax3.text(0, -0.20, "Femenino", ha="center", fontsize=8, color=SUB)
ax3.set_title("Distribucion por Genero", fontsize=11, fontweight="bold",
              color=TEXT, pad=8)
ax3.legend(
    handles=[mpatches.Patch(color=A3, label=f"Femenino  {fem:,}"),
             mpatches.Patch(color=A2, label=f"Masculino  {masc:,}")],
    loc="lower center", bbox_to_anchor=(0.5,-0.18), ncol=1,
    fontsize=8, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT
)

# ── FILA 2 col 1: Donut etario ───────────────────────────────────────────────
ax4 = fig.add_subplot(gs[2, 1])
ax4.set_facecolor(PANEL)
for sp in ax4.spines.values(): sp.set_visible(False)
pct_ad = adultos / (adultos + menores) * 100
ax4.pie([adultos, menores], colors=[A1, A4], startangle=90,
        wedgeprops={"width":0.52,"edgecolor":BG,"linewidth":2},
        explode=(0.03,0))
ax4.text(0, 0.10, f"{pct_ad:.1f}%", ha="center", va="center",
         fontsize=19, fontweight="bold", color=A1)
ax4.text(0, -0.20, "Adultos", ha="center", fontsize=8, color=SUB)
ax4.set_title("Distribucion por Grupo Etario", fontsize=11,
              fontweight="bold", color=TEXT, pad=8)
ax4.legend(
    handles=[mpatches.Patch(color=A1, label=f"Adultos   {adultos:,}"),
             mpatches.Patch(color=A4, label=f"Menores  {menores:,}")],
    loc="lower center", bbox_to_anchor=(0.5,-0.18), ncol=1,
    fontsize=8, facecolor=PANEL, edgecolor=BORDER, labelcolor=TEXT
)

# ── FILA 2 col 2-3: IPM vs Casos ────────────────────────────────────────────
ax5 = fig.add_subplot(gs[2, 2:4])
ax5.set_facecolor(PANEL)
for sp in ["top","right"]: ax5.spines[sp].set_visible(False)
for sp in ["bottom","left"]: ax5.spines[sp].set_color(BORDER)

sc = ax5.scatter(scatter["ipm"], scatter["casos"],
                 c=scatter["casos"], cmap="YlOrRd",
                 s=scatter["casos"]/scatter["casos"].max()*400+30,
                 alpha=0.85, edgecolors=BORDER, lw=0.6, zorder=3)
z = np.polyfit(scatter["ipm"], scatter["casos"], 1)
xl = np.linspace(scatter["ipm"].min(), scatter["ipm"].max(), 100)
ax5.plot(xl, np.poly1d(z)(xl), color=A1, ls="--", alpha=0.55, lw=1.5)
for _, row in scatter.nlargest(5,"casos").iterrows():
    ax5.annotate(row["dpto"], (row["ipm"], row["casos"]),
                 textcoords="offset points", xytext=(6,4),
                 fontsize=7, color=TEXT, alpha=0.9)
ax5.set_title("IPM Promedio vs Total de Casos por Departamento",
              fontsize=12, fontweight="bold", color=TEXT, pad=10, loc="left")
ax5.set_xlabel("IPM Promedio (%)", color=SUB, fontsize=9)
ax5.set_ylabel("Total Casos Acumulados", color=SUB, fontsize=9)
ax5.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f"{int(x):,}"))
ax5.tick_params(colors=SUB, labelsize=8)
ax5.grid(True, color=GRID, lw=0.6, alpha=0.7)
cbar = plt.colorbar(sc, ax=ax5, pad=0.02)
cbar.ax.yaxis.set_tick_params(color=SUB, labelsize=7)
cbar.set_label("Total Casos", color=SUB, fontsize=8)

# Footer
fig.text(
    0.04, 0.014,
    "Fuentes: Policia Nacional (datos.gov.co) | DANE IPM Departamental | API DIVIPOLA"
    "   |   Data Warehouse: Supabase/PostgreSQL   |   ETL G51 - Universidad Autonoma de Occidente - Juan Jose Alban 2235677",
    fontsize=7.5, color=SUB, alpha=0.8
)

out = "visualizations/08_dashboard_ejecutivo.png"
fig.savefig(out, dpi=160, bbox_inches="tight", facecolor=BG)
plt.close(fig)
print("Dashboard guardado:", out)
