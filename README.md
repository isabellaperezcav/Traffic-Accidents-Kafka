# Traffic Accidents ETL con Kafka

## Descripción General

El proyecto **Traffic Accidents ETL con Kafka** es una solución avanzada de pipeline de datos, diseñada para procesar, analizar y visualizar datos de accidentes de tráfico en tiempo real. Integra **Apache Airflow** para la orquestación de flujos de trabajo, **Kafka** para el streaming de datos, y **Streamlit** para la visualización en tiempo real. El pipeline extrae datos de un CSV de **Kaggle** (migrado a **PostgreSQL**) y los enriquece con información geoespacial de **OpenStreetMap (OSM)**, permitiendo análisis de patrones de accidentes y zonas de riesgo. Los datos transformados se almacenan en un modelo dimensional y se visualizan mediante dashboards en **Power BI** y **Streamlit**.

---

## Objetivos

* **Extraer**: Obtener datos de accidentes de tráfico desde PostgreSQL (`CrashTraffic`) y datos geoespaciales desde la API de OSM.
* **Transformar**: Limpiar, enriquecer y estructurar los datos dentro de un modelo dimensional.
* **Cargar**: Almacenar los datos transformados en PostgreSQL (`CrashTraffic_Dimensional`).
* **Orquestar**: Automatizar el pipeline utilizando Apache Airflow.
* **Analizar**: Realizar análisis exploratorios (EDA) sobre datos de accidentes y de OSM.
* **Visualizar**: Crear dashboards en Power BI y Streamlit.
* **Streaming**: Transmitir métricas con Kafka y visualizarlas en tiempo real mediante Streamlit.

---

## Fuente de Datos

El dataset principal proviene de **Kaggle** (*Traffic Accidents*), con más de **10,000 registros** sobre severidad, clima y datos temporales, migrado a PostgreSQL (`CrashTraffic`). Este se enriquece con datos geoespaciales de **OSM**.

---

## Pipeline ETL

### Extracción

* **Fuente**: Tabla `CrashTraffic` en PostgreSQL y API de OSM.
* **Método**: Scripts en Python y consultas SQL; OSM procesado en `API_EDA.ipynb`.

### Transformación

* **Limpieza**: Eliminación de nulos y duplicados, estandarización de categorías.
* **Ingeniería**: Conversión de fechas y creación de variables categóricas/binarias.
* **Enriquecimiento**: Integración con datos de OSM.
* **Modelo Dimensional**: Esquema en estrella para consultas eficientes.
* **Pruebas Unitarias**: Validación de transformaciones con `pytest`.
* **Great Expectations**: Validación de datos antes del merge.
* **Merge**: Combinación de datos entre la base y OSM.

### Carga

* **Destino**: `CrashTraffic_Dimensional` en PostgreSQL.
* **Método**: Carga mediante SQLAlchemy vía Airflow.

### Orquestación

* **Airflow**: DAG `etl_crash_traffic` para automatizar el proceso ETL.

### Streaming

* **Kafka**: Transmisión de métricas post-merge.
* **Consumidor**: Script en Python con Streamlit para visualización en tiempo real.

---

## Análisis Exploratorio (EDA)

* **API EDA (`API_EDA.ipynb`)**: Análisis de datos desde OSM.
* **Dataset EDA (`002_EDA_csv.ipynb`)**: Análisis univariado, bivariado y temporal de accidentes.

---

## Visualizaciones

* **Power BI**: Dashboard con insights clave (severidad, clima, tendencias).
* **Streamlit**: Dashboard en tiempo real mostrando métricas vía Kafka.

---

## Project Structure

```
📂 Traffic-Accidents-ETL
TRAFFICACCIDENTS/
├── API/
│   ├── data/
│   ├── notebooks/
│   │   ├── API_EDA.ipynb
│   │   └── git_attributes
├── config/
│   └── conexion_db.py
├── dags/
│   └── etl_crash_traffic.py
├── Dashboard/
├── data/
│   ├── CrashTraffic_clean.csv
│   └── CrashTraffic.csv
├─  logs/
├── notebooks/
│   ├── 001_extract.ipynb
│   ├── 002_EDA_csv.ipynb
├── pdf/
├── venv/
├── .env
├── .gitignore
├── docker-compose.yaml
├── README.md
└── requirements.txt
```

---

## Tecnologías

* **Lenguaje y procesamiento**: Python, pandas, numpy
* **Visualización**: matplotlib, seaborn, Power BI, Streamlit
* **Base de Datos**: PostgreSQL, psycopg2, SQLAlchemy
* **Orquestación**: Apache Airflow
* **Streaming**: Apache Kafka
* **Contenerización**: Docker, Docker Compose
* **Notebooks**: Jupyter
* **Versionado**: Git, GitHub
* **Pruebas**: pytest
* **Validación**: Great Expectations

---

## Destino Final

* **Base de Datos**: `CrashTraffic_Dimensional`
* **Dashboards**: Power BI y Streamlit

---

## Rendimiento y Hallazgos

* **Eficiencia**: Optimización para grandes volúmenes de datos.
* **Hallazgos**: Correlaciones entre condiciones adversas y lesiones, identificación de picos de accidentes y zonas de alto riesgo.

---

## Requerimientos Cumplidos

Cumple con los criterios del proyecto **"ETL class project - Final delivery"**:

* Fuentes: Kaggle y OSM
* DAG Airflow: ETL con modelo dimensional
* Streaming: Kafka para métricas
* Dashboards: Power BI y Streamlit
* Consumidor Kafka: Visualización en Streamlit
* GitHub: Repositorio con notebooks de EDA incluidos

---

## Instalación

1. **Clonar el repositorio**:

   ```bash
   git clone https://github.com/isabellaperezcav/Traffic-Accidents-Kafka.git
   cd Traffic-Accidents-Kafka
   ```

2. **Crear entorno virtual**:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. **Instalar dependencias**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar PostgreSQL**:

   ```sql
   CREATE DATABASE crash_traffic;
   CREATE DATABASE crash_traffic_dimensional;
   ```

   Actualizar `config/conexion_db.py`.

5. **Variables de entorno**: Crear archivo `.env` con:

   ```
   AIRFLOW__CORE__EXECUTOR=SequentialExecutor
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@localhost:5432/airflow
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=tu_usuario
   DB_NAME=tu_base_de_datos
   DB_PASSWORD=tu_contraseña
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

6. **Levantar Airflow con Docker**:

   ```bash
   docker-compose up -d
   ```

   Acceder a: [http://localhost:8080](http://localhost:8080)

7. **Configurar Kafka**: Crear topic y verificar ejecución.

8. **Ejecutar notebooks**:

   * `001_extract.ipynb`
   * `002_EDA_csv.ipynb`
   * `API_EDA.ipynb`

9. **Ejecutar pipeline**: Activar DAG `etl_crash_traffic` en Airflow.

10. **Kafka y Streamlit**:

    ```bash
    python kafka/producer.py
    python streamlit/app.py
    ```

---

## Uso

* **Pipeline**: Activar DAG en Airflow.
* **Dashboards**: Explorar visualizaciones en Power BI y Streamlit tras la ejecución.

---

## Mantenimiento

* **Monitoreo**: Desde la interfaz de Airflow.
* **Problemas comunes**:

  * Conexión: Verificar credenciales.
  * Airflow: Revisar dependencias.
  * Kafka: Asegurar creación de topic y ejecución del productor/consumidor.

---

## Conclusión

Este pipeline robusto integra herramientas avanzadas para ofrecer insights sobre accidentes de tráfico, apoyando estrategias de **seguridad vial** a través del análisis de datos en tiempo real.
