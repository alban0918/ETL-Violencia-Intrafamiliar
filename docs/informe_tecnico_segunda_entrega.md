# Informe Tecnico - Segunda Entrega ETL
## Violencia Intrafamiliar en Colombia

**Curso:** ETL (G51) - Universidad Autonoma de Occidente  
**Estudiante:** Juan Jose Alban - 2235677  
**Docente:** Daniel Felipe Romero  
**Fecha:** Abril 7, 2026  

---

## 1. Objetivos Refinados

### Por que construimos este pipeline?

En la primera entrega logramos descargar y explorar datos de violencia intrafamiliar de Colombia. Para esta segunda entrega, el objetivo fue automatizar todo ese proceso: que el pipeline corra solo, sin intervencion manual, y que ademas tenga un mecanismo de validacion que garantice que los datos que llegan al Data Warehouse son de calidad.

La pregunta que guia el analisis es: **Como se distribuye la violencia intrafamiliar en Colombia segun departamento, ano, genero y nivel de pobreza?**

### Preguntas especificas que el pipeline responde:

1. Que departamentos concentran mas casos de violencia intrafamiliar?
2. Como ha evolucionado la violencia intrafamiliar entre 2010 y 2025?
3. Existe relacion entre el Indice de Pobreza Multidimensional (IPM) y la cantidad de casos?
4. Cual es la proporcion de victimas femeninas vs masculinas?
5. Los adultos son el grupo mas afectado o tambien hay una proporcion significativa de menores?

### Alineacion con ODS:
- **ODS 5 - Igualdad de Genero:** el 77.8% de las victimas son mujeres, lo que hace urgente analizar la dimension de genero
- **ODS 16 - Paz, Justicia e Instituciones Solidas:** la violencia intrafamiliar es un indicador directo de ruptura del tejido social

---

## 2. Fuentes de Datos

### Fuente 1 - CSV Policia Nacional (primera entrega)
- **Origen:** datos.gov.co - "Reporte Delito Violencia Intrafamiliar, Policia Nacional"
- **URL:** `https://www.datos.gov.co/resource/sqer-ipyf.csv`
- **Formato:** CSV descargado directamente via requests
- **Registros:** 660,756 filas x 9 columnas
- **Columnas clave:** departamento, municipio, fecha_hecho, genero, grupo_etario, cantidad
- **Tarea Airflow:** `extract_violencia`

### Fuente 2 - Excel DANE IPM Departamental (dataset adicional)
- **Origen:** DANE - Estadisticas de Pobreza Multidimensional Departamental 2024
- **URL base:** `https://www.dane.gov.co/index.php/estadisticas-por-tema/pobreza-y-condiciones-de-vida/pobreza-multidimensional`
- **Formato:** Excel (.xlsx), hoja "IPM_Departamentos", transformado a formato largo
- **Registros:** 231 filas (32 departamentos x ~7 anos)
- **Columnas clave:** departamento, anio, ipm_total
- **Por que se eligio:** el IPM permite cruzar la violencia con el nivel de pobreza por departamento, respondiendo si los departamentos mas pobres son tambien los mas violentos
- **Tarea Airflow:** `extract_dane`

### Fuente 3 - API DIVIPOLA (API publica)
- **Origen:** API publica datos.gov.co - Division Politico Administrativa de Colombia
- **Endpoint:** `https://www.datos.gov.co/resource/gdxc-w37w.json?$limit=5000`
- **Formato:** JSON REST API, consultada con requests
- **Registros:** 1,122 municipios con coordenadas geograficas
- **Columnas clave:** cod_dpto, dpto, cod_mpio, nom_mpio, longitud, latitud
- **Por que se eligio la API DIVIPOLA:** es la API oficial del DANE para codigos geograficos de Colombia. Permite estandarizar los nombres de departamentos usando el codigo oficial y agrega coordenadas geograficas al dataset, lo que habilita visualizaciones de mapa y enriquece el analisis geografico.
- **Tarea Airflow:** `extract_api`

---

## 3. Analisis Exploratorio de Datos (EDA)

El EDA fue realizado en notebooks de Google Colab (disponibles en la carpeta `notebooks/`).

### Fuente 1 - Violencia Intrafamiliar

| Metrica | Valor |
|---|---|
| Total de filas | 660,756 |
| Columnas | 9 |
| Rango temporal | 2010 - 2025 |
| Valores nulos en genero | < 1% |
| Departamentos unicos | 34 (incluye "SIN INFORMACION") |

**Hallazgos principales:**
- El ano 2024 registra el pico historico de casos
- Antioquia, Valle del Cauca y Bogota concentran mas del 40% de los casos nacionales
- El 77.8% de las victimas son mujeres
- El 96.8% de las victimas son adultos

**Problemas encontrados y solucion:**
- Nombres de departamentos inconsistentes: "GUAJIRA" en vez de "LA GUAJIRA", "BOGOTA D.C." en vez de "BOGOTA, D.C." -> solucionado con mapa de homologacion
- Columna `fecha_hecho` con formatos mixtos -> solucionado con `format="mixed"` en pandas
- Columna `cantidad` con algunos valores nulos -> imputados a 0

### Fuente 2 - DANE IPM

| Metrica | Valor |
|---|---|
| Total de filas | 231 |
| Departamentos | 32 |
| Rango temporal | 2018 - 2023 |
| Valores nulos | 0 |

**Hallazgos:**
- El IPM varia entre 3% (Bogota) y 70%+ (Choco, Vaupes)
- Solo hay datos de IPM para 6 anos, por eso al hacer el merge quedan 287 valores nulos en `ipm_total` (esperado y documentado)

### Fuente 3 - DIVIPOLA

| Metrica | Valor |
|---|---|
| Total de municipios | 1,122 |
| Departamentos | 32 |
| Campos con coordenadas | 100% |

### Visualizaciones del EDA

Los graficos estan disponibles en `visualizations/`:

| Archivo | Descripcion |
|---|---|
| `01_evolucion_anual.png` | Tendencia temporal de casos 2010-2025 |
| `02_top10_departamentos.png` | Ranking de departamentos por casos |
| `03_distribucion_genero.png` | Proporcion femenino/masculino |
| `04_distribucion_etaria.png` | Adultos vs menores |
| `05_ipm_vs_casos.png` | Correlacion pobreza-violencia |
| `06_heatmap_depto_anio.png` | Mapa de calor departamento x ano |
| `07_airflow_dag_diagram.png` | Estructura visual del DAG |
| `08_dashboard_ejecutivo.png` | Dashboard ejecutivo integrado |

---

## 4. Transformacion de Datos

El modulo `src/transform.py` aplica las siguientes transformaciones en orden:

### 4.1 Normalizacion de texto
Se eliminan tildes, se convierte a mayusculas y se eliminan sufijos como "(CT)" para estandarizar los nombres de departamentos entre las tres fuentes.

### 4.2 Agregacion de violencia
Se agruparon los 660,756 registros por `departamento` y `anio`, calculando:
- `total_casos`: suma total
- `casos_femenino` / `casos_masculino`: por genero
- `casos_adultos` / `casos_menores`: por grupo etario

Resultado: **511 registros** (32 departamentos x 16 anos)

### 4.3 Homologacion territorial

| Nombre original en policia | Nombre estandar DANE |
|---|---|
| GUAJIRA | LA GUAJIRA |
| SAN ANDRES | SAN ANDRES, PROVIDENCIA Y SANTA CATALINA |
| VALLE | VALLE DEL CAUCA |
| BOGOTA D.C. | BOGOTA, D.C. |
| BOGOTA DC | BOGOTA, D.C. |

### 4.4 Merge de las tres fuentes

```
violencia_agregada (511 x 3)
      |
      +-- LEFT JOIN por departamento_limpio --> + DIVIPOLA (cod_dpto, coordenadas)
      |
      +-- LEFT JOIN por departamento + anio  --> + DANE IPM (ipm_total)
      |
      v
DATASET FINAL: 511 registros x 10 columnas
```

**Archivo final:** `data/final/04_dataset_final_enriquecido.csv`

---

## 5. Diagrama de Arquitectura

```
+------------------+   +------------------+   +------------------+
| FUENTE 1         |   | FUENTE 2         |   | FUENTE 3         |
| CSV Policia Nal. |   | Excel DANE IPM   |   | API DIVIPOLA     |
| datos.gov.co     |   | dane.gov.co      |   | datos.gov.co     |
+--------+---------+   +--------+---------+   +--------+---------+
         |                      |                      |
         v                      v                      v
+--------+-------+   +----------+-------+   +---------+---------+
| extract_       |   | extract_dane     |   | extract_api       |
| violencia      |   | (Airflow Task 2) |   | (Airflow Task 3)  |
| (Task 1)       |   +------------------+   +-------------------+
+----------------+             |                      |
         |                     +----------+-----------+
         +-----------------------------------+
                                            |
                               +------------+----------+
                               | transform             |
                               | (Airflow Task 4)      |
                               | pandas merge/clean    |
                               +------------+----------+
                                            |
                               +------------+----------+
                               | validate              |
                               | (Airflow Task 5)      |
                               | Great Expectations    |
                               | 13 validaciones       |
                               | PASS->continua        |
                               | FAIL->pipeline para   |
                               +------------+----------+
                                            |
                               +------------+----------+
                               | load                  |
                               | (Airflow Task 6)      |
                               | SQLAlchemy+Supabase   |
                               +------------+----------+
                                            |
                          +-----------------+-----------------+
                          | DATA WAREHOUSE - Supabase/PostgreSQL |
                          | fact_violencia    (511 rows)         |
                          | dim_departamento  ( 32 rows)         |
                          | dim_tiempo        ( 16 rows)         |
                          +------------------+------------------+
                                            |
                          +-----------------+-----------------+
                          | VISUALIZACIONES                   |
                          | Python matplotlib + Dashboard     |
                          +-----------------------------------+
```

---

## 6. Diseno del DAG de Airflow

**Archivo:** `dags/etl_violencia_pipeline.py`

**Configuracion del DAG:**

| Parametro | Valor |
|---|---|
| dag_id | etl_violencia_intrafamiliar |
| schedule_interval | @daily |
| start_date | 2025-01-01 |
| catchup | False |
| max_active_runs | 1 |
| owner | juan_jose_alban |
| retries | 1 (con 5 min de espera) |

**Estructura de dependencias:**
```
[extract_violencia] --+
[extract_dane]        +--> [transform] --> [validate] --> [load]
[extract_api]       --+
```

Las tres extracciones corren en **paralelo** para reducir el tiempo total. Solo cuando las tres terminan exitosamente se dispara `transform`.

**Comunicacion entre tareas (XCom):**
- Cada extraccion empuja el shape del DataFrame al XCom para trazabilidad
- `validate` empuja `validation_passed=True` si las 13 validaciones pasan
- `load` verifica ese valor y lanza `AirflowSkipException` si es False

---

## 7. Validacion con Great Expectations

**Version:** Great Expectations 1.15.x  
**Modo:** Contexto efimero (sin servidor, compatible con Python 3.12+)

### Suite: `suite_violencia` - 13 expectativas

| # | Expectativa | Columna | Tipo | Resultado |
|---|---|---|---|---|
| 1 | No nulos | cod_dpto | Critica | PASS |
| 2 | No nulos | dpto | Critica | PASS |
| 3 | No nulos | anio | Critica | PASS |
| 4 | No nulos | total_casos | Critica | PASS |
| 5 | Valores >= 0 | total_casos | Critica | PASS |
| 6 | Valores >= 0 | casos_femenino | Critica | PASS |
| 7 | Valores >= 0 | casos_masculino | Critica | PASS |
| 8 | Rango 2010-2025 | anio | Critica | PASS |
| 9 | Valores > 0 | total_casos | Critica | PASS |
| 10 | Combinacion unica | cod_dpto + anio | Critica | PASS |
| 11 | Valores >= 0 | casos_adultos | Critica | PASS |
| 12 | Valores >= 0 | casos_menores | Critica | PASS |
| 13 | Rango 0-100 | ipm_total | Tolerante (mostly=0.40) | PASS |

**Resultado final: 13/13 validaciones PASS**

**Nota sobre expectativa 13:** el IPM del DANE solo esta disponible para 6 anos (2018-2023). Los 287 nulos restantes corresponden a anos donde el DANE no tiene datos publicados. Por eso se usa `mostly=0.40` en lugar de rechazar todos los nulos.

---

## 8. Modelo de Datos - Star Schema

```
            +---------------+
            |   dim_tiempo  |
            |---------------|
            | id_tiempo PK  |
            | anio          |
            +-------+-------+
                    |
        +-----------+-----------+
        |      fact_violencia   |
+-------+-------+  |           |
|dim_departamento|  | id_viol PK|
|---------------|  | id_dpto FK|
| id_depto PK   +--+ id_tpo FK |
| cod_dpto      |  | total_cas |
| dpto          |  | casos_fem |
| longitud      |  | casos_mas |
| latitud       |  | casos_adu |
+---------------+  | casos_men |
                   | ipm_total |
                   +-----------+
```

**Carga al Data Warehouse:**
- Supabase (PostgreSQL gestionado en AWS us-east-1)
- UPSERT con `ON CONFLICT DO UPDATE` para evitar duplicados en re-ejecuciones
- 511 registros de hechos, 32 departamentos, 16 periodos temporales

---

## 9. Supuestos y Decisiones

1. **Homologacion manual de nombres:** los nombres de departamentos en la fuente policial no coinciden con los del DANE. Se construyo un diccionario de mapeo validado contra DIVIPOLA.

2. **ipm_total con nulos aceptados:** el DANE solo publica IPM desde 2018. Los anos anteriores no tienen datos disponibles. Documentado y manejado con `mostly=0.40` en GE.

3. **Airflow en Windows con stubs POSIX:** Airflow no soporta Windows nativamente. Se implementaron stubs para modulos POSIX (`pwd`, `resource`) y se uso Waitress como servidor WSGI en lugar de Gunicorn. Para produccion se recomienda WSL2 o Docker.

4. **Cache local de archivos raw:** si los archivos ya existen en `data/raw/`, el pipeline los usa sin re-descargar. Esto permite desarrollar y probar sin consumir ancho de banda.

5. **Agregacion departamento-ano:** se decidio agregar desde el nivel municipio-hecho al nivel departamento-ano para poder hacer el join con el IPM del DANE, que solo existe a nivel departamental y anual.

---

## 10. Ejemplos de Visualizaciones e Insights

### Dashboard ejecutivo (`visualizations/08_dashboard_ejecutivo.png`)

El dashboard integra los 5 graficos principales en una sola vista ejecutiva:

**KPIs principales:**
- 1,391,346 casos totales en el periodo 2010-2025
- 77.8% de victimas son mujeres
- Ano pico: 2024
- 32 departamentos analizados

**Insight 1 - Tendencia creciente con caida en 2020:**  
Se observa un crecimiento sostenido desde 2010. La caida en 2020 puede atribuirse al subregistro durante la pandemia (restricciones para denunciar). La recuperacion post-2021 fue mas pronunciada que antes de la pandemia.

**Insight 2 - Concentracion en centros urbanos:**  
Antioquia, Bogota y Valle del Cauca concentran mas del 40% de los casos. Esto refleja tanto el tamano de la poblacion como la mayor capacidad institucional para registrar denuncias en zonas urbanas.

**Insight 3 - Dimension de genero critica:**  
Con 1,079,827 casos femeninos vs 308,325 masculinos, la violencia intrafamiliar en Colombia tiene un sesgo de genero muy claro. Este dato respalda politicas publicas focalizadas en la proteccion de la mujer (ODS 5).

**Insight 4 - IPM no determina linealmente los casos:**  
Los departamentos con mas casos absolutos (Antioquia, Bogota) tienen IPM bajo porque son los mas urbanizados. Los departamentos con IPM alto (Choco, Amazonas) tienen menos casos absolutos pero posiblemente mayor subregistro por barreras de acceso institucional.

**Insight 5 - Adultos como grupo principal:**  
El 96.8% de las victimas son adultos. Sin embargo, los 43,048 casos de menores representan una problematica de proteccion infantil que merece atencion especifica.

---

*Informe elaborado para la Segunda Entrega del Proyecto ETL - ETL G51, Universidad Autonoma de Occidente, Abril 2026.*
