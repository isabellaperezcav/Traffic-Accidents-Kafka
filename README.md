# Traffic Accidents ETL with Kafka

## Overview

The **Traffic Accidents ETL with Kafka** project is an advanced data pipeline solution designed to process, analyze, and visualize traffic accident data in real time. It integrates **Apache Airflow** for workflow orchestration, **Kafka** for data streaming, and **Streamlit** for real-time visualization. The pipeline extracts data from a **Kaggle** CSV (migrated to **PostgreSQL**) and enriches it with geospatial information from **OpenStreetMap (OSM)**, enabling analysis of accident patterns and risk zones. Transformed data is stored in a dimensional model and visualized through dashboards in **Power BI** and **Streamlit**.

---

## Objectives

* **Extract**: Retrieve traffic accident data from PostgreSQL (`CrashTraffic`) and geospatial data from the OSM API.
* **Transform**: Clean, enrich, and structure the data within a dimensional model.
* **Load**: Store the transformed data in PostgreSQL (`CrashTraffic_Dimensional`).
* **Orchestrate**: Automate the pipeline using Apache Airflow.
* **Analyze**: Perform exploratory data analysis (EDA) on accident and OSM data.
* **Visualize**: Create dashboards in Power BI and Streamlit.
* **Streaming**: Stream metrics with Kafka and visualize them in real time via Streamlit.

---

## Data Source

The main dataset comes from **Kaggle** (*Traffic Accidents*), with over **10,000 records** on severity, weather, and temporal data, migrated to PostgreSQL (`CrashTraffic`). This is enriched with geospatial data from **OSM**.

---

## ETL Pipeline

### Extraction

* **Source**: `CrashTraffic` table in PostgreSQL and OSM API.
* **Method**: Python scripts and SQL queries; OSM processed in `API_EDA.ipynb`.

### Transformation

* **Cleaning**: Removal of nulls and duplicates, category standardization.
* **Engineering**: Date conversion and creation of categorical/binary variables.
* **Enrichment**: Integration with OSM data.
* **Dimensional Model**: Star schema for efficient querying.
* **Unit Testing**: Transformation validation with `pytest`.
* **Great Expectations**: Data validation before merging.
* **Merge**: Combine base and OSM data.

### Load

* **Target**: `CrashTraffic_Dimensional` in PostgreSQL.
* **Method**: Load via SQLAlchemy through Airflow.

### Orchestration

* **Airflow**: DAG `etl_crash_traffic` to automate the ETL process.

### Streaming

* **Kafka**: Stream post-merge metrics.
* **Consumer**: Python script with Streamlit for real-time visualization.

---

## Exploratory Data Analysis (EDA)

* **API EDA (`API_EDA.ipynb`)**: Data analysis from OSM.
* **Dataset EDA (`002_EDA_csv.ipynb`)**: Univariate, bivariate, and temporal accident analysis.

---

## Visualizations

* **Power BI**: Dashboard with key insights (severity, weather, trends).
* **Streamlit**: Real-time dashboard showing metrics via Kafka.

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
├── great_expectations/
├── kafka/
│   ├── streamlit_kafka_consumer.py
├─  logs/
├── notebooks/
│   ├── 001_extract.ipynb
│   ├── 002_EDA_csv.ipynb
├── pdf/
├── tests/
│   ├── test_dag_api_etl.py
│   ├── test_dag_db_etl.py
├── venv/
├── .env
├── .gitignore
├── docker-compose.yaml
├── proyecto03.code-workspace
├── README.md
└── requirements.txt
```

---

## Technologies

* **Language and Processing**: Python, pandas, numpy
* **Visualization**: matplotlib, seaborn, Power BI, Streamlit
* **Database**: PostgreSQL, psycopg2, SQLAlchemy
* **Orchestration**: Apache Airflow
* **Streaming**: Apache Kafka
* **Containerization**: Docker, Docker Compose
* **Notebooks**: Jupyter
* **Version Control**: Git, GitHub
* **Testing**: pytest
* **Validation**: Great Expectations

---

## Final Destination

* **Database**: `CrashTraffic_Dimensional`
* **Dashboards**: Power BI and Streamlit

---

## Performance and Findings

* **Efficiency**: Optimized for large data volumes.
* **Findings**: Correlations between adverse conditions and injuries, identification of accident peaks and high-risk zones.

---

## Requirements Fulfilled

Meets the criteria of the **"ETL class project - Final delivery"**:

* Sources: Kaggle and OSM
* Airflow DAG: ETL with dimensional model
* Streaming: Kafka for metrics
* Dashboards: Power BI and Streamlit
* Kafka Consumer: Visualization in Streamlit
* GitHub: Repository with EDA notebooks included

---

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/isabellaperezcav/Traffic-Accidents-Kafka.git
   cd Traffic-Accidents-Kafka
   ```

2. **Create virtual environment**:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configure PostgreSQL**:

   ```sql
   CREATE DATABASE crash_traffic;
   CREATE DATABASE crash_traffic_dimensional;
   ```

   Update `config/conexion_db.py`.

5. **Environment variables**: Create `.env` file with:

   ```
   AIRFLOW__CORE__EXECUTOR=SequentialExecutor
   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://user:password@localhost:5432/airflow
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=your_user
   DB_NAME=your_database
   DB_PASSWORD=your_password
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

6. **Start Airflow with Docker**:

   ```bash
   docker-compose up -d
   ```

   Access at: [http://localhost:8080](http://localhost:8080)

7. **Configure Kafka**: Create topic and verify execution.

8. **Run notebooks**:

   * `001_extract.ipynb`
   * `002_EDA_csv.ipynb`
   * `API_EDA.ipynb`

9. **Run pipeline**: Activate DAG `etl_crash_traffic` in Airflow.

10. **Kafka and Streamlit**:

    ```bash
    python kafka/producer.py
    python streamlit/app.py
    ```

---

## Usage

* **Pipeline**: Activate DAG in Airflow.
* **Dashboards**: Explore visualizations in Power BI and Streamlit after execution.

---

## Maintenance

* **Monitoring**: From the Airflow interface.
* **Common issues**:

  * Connection: Check credentials.
  * Airflow: Review dependencies.
  * Kafka: Ensure topic creation and producer/consumer execution.

---

## Conclusion

This robust pipeline integrates advanced tools to deliver insights on traffic accidents, supporting **road safety** strategies through real-time data analysis.

---
