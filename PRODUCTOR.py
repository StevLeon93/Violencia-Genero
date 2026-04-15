import time
import json
import pandas as pd
from kafka import KafkaProducer

# Cargar dataset real
df = pd.read_csv("violencia_genero.csv")

# Convertir a lista de registros
data = df.to_dict(orient="records")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Enviar datos simulando tiempo real
for fila in data:
    producer.send('violencia_genero', value=fila)
    print("Enviado:", fila)
    time.sleep(1)  # simula streaming