# Proyecto ETL – Violencia Intrafamiliar en Colombia

## Descripción
Este proyecto implementa un proceso completo de ETL (Extract, Transform, Load) utilizando un conjunto de datos público sobre violencia intrafamiliar en Colombia, alineado con los Objetivos de Desarrollo Sostenible (ODS).

El objetivo es analizar tendencias por género, grupo etario y tiempo mediante un dashboard interactivo en Power BI conectado a una base de datos relacional.

Repositorio:
https://github.com/alban0918/ETL-Violencia-Intrafamiliar

---

## Objetivos
- Implementar un pipeline ETL completo.
- Migrar los datos a una base de datos relacional.
- Realizar análisis exploratorio de datos (EDA).
- Crear visualizaciones significativas.
- Alinear el análisis con los ODS 5 y 16.

---

## Dataset
Fuente: datos.gov.co
Nombre: Reporte Delito Violencia Intrafamiliar – Policía Nacional
Registros: más de 660,000

Variables principales:
- Departamento
- Municipio
- Género
- Grupo etario
- Fecha del hecho
- Cantidad de casos

---

## Arquitectura
El proyecto sigue una arquitectura ETL:

1. Extracción del CSV desde datos.gov.co
2. Transformación en Google Colab (Python)
3. Carga en base de datos PostgreSQL (Supabase)
4. Visualización en Power BI

Modelo de datos: Esquema en estrella (Star Schema)

---

## Tecnologías utilizadas
- Python
- Google Colab
- SQLite (fase de prueba)
- PostgreSQL (Supabase)
- Power BI
- GitHub

---

## Visualizaciones
El dashboard incluye:
- Casos por año (gráfico de líneas)
- Casos por género (gráfico circular)
- Casos por grupo etario
- Casos por departamento
- Indicadores clave (KPI)

---

## Ejecución del proyecto
1. Ejecutar el notebook ubicado en la carpeta `/notebooks`.
2. Conectar Power BI a la base de datos PostgreSQL.
3. Abrir el archivo `.pbix` para visualizar el dashboard.

---

## Estructura del repositorio
