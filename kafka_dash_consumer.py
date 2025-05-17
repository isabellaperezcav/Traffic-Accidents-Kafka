# kafka_dashboard_consumer.py

import json
import os
import logging
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Configura el logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cargar variables de entorno (para obtener la configuración de Kafka)
load_dotenv()

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
# ¡Importante! Define un ID de grupo para tu consumidor
# Kafka usa esto para rastrear qué mensajes ha leído este grupo de consumidores.
# Si reinicias el consumidor con el mismo group_id, continuará desde donde se quedó.
# Si quieres que varios consumidores trabajen en paralelo en el mismo topic, dales el mismo group_id.
CONSUMER_GROUP_ID = 'dashboard-realtime-group'

def create_kafka_consumer():
    """Crea y retorna una instancia de KafkaConsumer."""
    if not KAFKA_SERVERS or not KAFKA_TOPIC:
        logger.error("Error: KAFKA_BOOTSTRAP_SERVERS o KAFKA_TOPIC no están configurados en las variables de entorno.")
        return None

    logger.info(f"Intentando conectar al servidor Kafka: {KAFKA_SERVERS}")
    logger.info(f"Escuchando el topic: {KAFKA_TOPIC}")
    logger.info(f"Usando Consumer Group ID: {CONSUMER_GROUP_ID}")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVERS.split(','),
            group_id=CONSUMER_GROUP_ID,
            # Cómo deserializar el mensaje (de bytes a dict Python, asumiendo que el productor envió JSON)
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            # 'earliest': Empezar desde el principio del topic si es un nuevo group_id.
            # 'latest': Empezar desde los mensajes nuevos (generalmente preferido para dashboards en tiempo real).
            auto_offset_reset='latest',
            # Opcional: Otras configuraciones (seguridad, timeouts, etc.)
            # security_protocol='SASL_SSL',
            # sasl_mechanism='PLAIN',
            # sasl_plain_username='tu_usuario',
            # sasl_plain_password='tu_password',
            consumer_timeout_ms=1000 # Descomentar si quieres que el bucle no bloquee indefinidamente
        )
        logger.info("Conexión con Kafka establecida exitosamente.")
        return consumer
    except Exception as e:
        logger.error(f"Error al conectar o crear el consumidor de Kafka: {e}", exc_info=True)
        return None

def run_consumer():
    """Inicia el bucle del consumidor para procesar mensajes."""
    consumer = create_kafka_consumer()
    if not consumer:
        logger.error("No se pudo crear el consumidor. Saliendo.")
        return

    logger.info("Consumidor iniciado. Esperando mensajes...")
    try:
        # El consumidor actúa como un iterador que bloquea hasta que lleguen nuevos mensajes
        for message in consumer:
            # message contiene metadatos como topic, partition, offset, key, value
            logger.info(f"Mensaje recibido - Offset: {message.offset}, Key: {message.key}")

            try:
                # El 'value' ya está deserializado por value_deserializer
                data = message.value
                logger.debug(f"Datos del mensaje: {data}")

                # --- AQUÍ VA LA LÓGICA PARA ACTUALIZAR TU DASHBOARD ---
                # Ejemplo:
                # 1. Guardar en una base de datos optimizada para lectura rápida (ej. Redis, InfluxDB, TimescaleDB)
                #    save_to_timeseries_db(data)
                # 2. Enviar directamente a los clientes del dashboard vía WebSockets
                #    push_to_websockets(data)
                # 3. Realizar algún cálculo en tiempo real
                #    update_realtime_metrics(data)

                print(f"Procesado mensaje con crash_id: {data.get('crash_id', 'N/A')}") # Solo para demostración

            except json.JSONDecodeError:
                logger.error(f"Error al decodificar JSON del mensaje. Offset: {message.offset}. Valor crudo: {message.value}")
            except Exception as e:
                logger.error(f"Error al procesar mensaje (Offset: {message.offset}): {e}", exc_info=True)

            # No necesitas 'consumer.commit()' explícito a menos que deshabilites 'enable_auto_commit' (default es True)

    except KeyboardInterrupt:
        logger.info("Interrupción detectada. Cerrando consumidor...")
    except Exception as e:
        logger.error(f"Error inesperado en el bucle del consumidor: {e}", exc_info=True)
    finally:
        if consumer:
            logger.info("Cerrando la conexión del consumidor.")
            consumer.close()

if __name__ == "__main__":
    run_consumer()