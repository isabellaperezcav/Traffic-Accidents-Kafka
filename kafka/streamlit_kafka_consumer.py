import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import time
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de Streamlit
st.set_page_config(page_title="Dashboard de Accidentes", layout="wide")
st.title("🚦 Dashboard de Accidentes en Tiempo Real")

# Configuración de Kafka desde .env
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "accidentes_stream")

# Estado de sesión para guardar los datos
if "eventos" not in st.session_state:
    st.session_state.eventos = []

# Inicializar el consumidor Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='streamlit-dashboard-group'
)

# Contenedor para la tabla
placeholder = st.empty()

# Leer algunos mensajes nuevos (no usar bucle infinito)
msg_pack = consumer.poll(timeout_ms=1000, max_records=10)
for tp, messages in msg_pack.items():
    for message in messages:
        try:
            evento = message.value
            st.session_state.eventos.append(evento)
        except Exception as e:
            st.error(f"Error al procesar el mensaje: {e}")

# Limitar a los últimos 100 registros
df = pd.DataFrame(st.session_state.eventos[-100:])

# Mostrar tabla y gráficas
with placeholder.container():
    st.subheader("📋 Últimos eventos de accidentes")
    st.dataframe(df)

    if not df.empty:

        # Gráfica: Total de heridos por tipo de lesión
        st.subheader("🚑 Lesiones Reportadas")
        lesion_cols = [
            'injuries_fatal',
            'injuries_incapacitating',
            'injuries_non_incapacitating',
            'injuries_reported_not_evident',
            'injuries_no_indication'
        ]
        if all(col in df.columns for col in lesion_cols):
            lesion_totals = df[lesion_cols].sum()
            st.bar_chart(lesion_totals)

        # Gráfica: Accidentes por condición climática
        if 'weather_condition' in df.columns:
            st.subheader("☁️ Condiciones Climáticas")
            st.bar_chart(df['weather_condition'].value_counts())

        # Gráfica: Tipo de primer choque
        if 'first_crash_type' in df.columns:
            st.subheader("💥 Tipo de Primer Impacto")
            st.bar_chart(df['first_crash_type'].value_counts())

        # Gráfica: Accidentes por hora
        if 'crash_hour' in df.columns:
            st.subheader("⏰ Accidentes por Hora")
            st.line_chart(df['crash_hour'].value_counts().sort_index())

        # Gráfica: Día de la semana
        if 'crash_day_of_week' in df.columns:
            st.subheader("📆 Accidentes por Día de la Semana")
            dias = {
                1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
                5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
            }
            df['crash_day_of_week'] = df['crash_day_of_week'].map(dias)
            st.bar_chart(df['crash_day_of_week'].value_counts())

        # Gráfica: Superficie vial
        if 'roadway_surface_cond' in df.columns:
            st.subheader("🛣️ Superficie Vial")
            st.bar_chart(df['roadway_surface_cond'].value_counts())

        # Gráfica: Causa principal
        if 'prim_contributory_cause' in df.columns:
            st.subheader("⚠️ Causas Principales del Accidente")
            st.bar_chart(df['prim_contributory_cause'].value_counts().head(10))

        # Mapa de ubicación
        if 'latitude' in df.columns and 'longitude' in df.columns:
            st.subheader("🗺️ Mapa de Ubicación de Accidentes")
            st.map(df[['latitude', 'longitude']])

# Esperar 2 segundos y recargar
time.sleep(2)
st.experimental_rerun()
