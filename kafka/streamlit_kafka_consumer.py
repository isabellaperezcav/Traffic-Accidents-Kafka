import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

st.set_page_config(page_title="Dashboard de Accidentes", layout="wide")
st.title("🚦 Dashboard de Accidentes en Tiempo Real")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "accidentes_stream")

# Estado de sesión para almacenar los datos
if "eventos" not in st.session_state:
    st.session_state.eventos = []

# Refrescar cada 5 segundos para recibir nuevos mensajes y actualizar gráficos
st_autorefresh(interval=5000, limit=None, key="auto_refresh")

# Inicializar KafkaConsumer UNA sola vez
if "consumer" not in st.session_state:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='streamlit-dashboard-group',
            max_poll_records=100
        )
        st.session_state.consumer = consumer
        logger.info("Kafka consumer inicializado correctamente")
    except NoBrokersAvailable as e:
        st.error(f"No se pudo conectar a Kafka: {e}")
        st.stop()

consumer = st.session_state.consumer

# Leer mensajes nuevos del topic
try:
    msg_pack = consumer.poll(timeout_ms=1000, max_records=100)
    for tp, messages in msg_pack.items():
        for message in messages:
            evento = message.value
            st.session_state.eventos.append(evento)
except KafkaError as e:
    st.error(f"Error en la comunicación con Kafka: {e}")

# Crear DataFrame con todos los datos recibidos
df = pd.DataFrame(st.session_state.eventos)

st.subheader("📋 Últimos eventos de accidentes")
if df.empty:
    st.write("No se han recibido eventos aún.")
else:
    st.dataframe(df.tail(20))  # Muestra últimos 20 eventos

    # Gráficos adaptativos según las columnas presentes

    # Mapa (usar latitud y longitud si están)
    lat_cols = ['Start_Lat', 'latitude']
    lon_cols = ['Start_Lng', 'longitude']
    lat_col = next((c for c in lat_cols if c in df.columns), None)
    lon_col = next((c for c in lon_cols if c in df.columns), None)
    if lat_col and lon_col:
        st.subheader("🗺️ Mapa de Accidentes")
        # Rename columns to match st.map expectations
        map_df = df[[lat_col, lon_col]].dropna().rename(columns={lat_col: 'latitude', lon_col: 'longitude'})
        st.map(map_df)

    # Columnas categóricas y numéricas para graficar
    cat_cols = []
    num_cols = []
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            num_cols.append(col)
        else:
            cat_cols.append(col)

    # Mostrar gráficos de barras para categóricas
    for c in cat_cols:
        # Mostrar solo si no es columna con muchos valores únicos (para evitar gráficos enormes)
        if df[c].nunique() <= 30:
            st.subheader(f"Distribución de {c}")
            st.bar_chart(df[c].value_counts())

    # Mostrar histogramas o conteos para numéricas
    for n in num_cols:
        if df[n].nunique() <= 30:
            st.subheader(f"Conteo de valores para {n}")
            st.bar_chart(df[n].value_counts())
        else:
            st.subheader(f"Histograma para {n}")
            st.bar_chart(df[n].dropna())