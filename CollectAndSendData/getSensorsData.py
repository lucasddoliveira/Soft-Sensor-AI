import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import pytz
import time
import threading
import pandas as pd

counter = 0
leitura = []
df = pd.DataFrame(columns=["Variable 1", "Variable 2", "Variable 3", "Variable 4", "Variable 5"])

def on_message(client, userdata, message):
    global counter, leitura, df  # Add these lines to declare counter, leitura, and df as global variables
    payload = message.payload.decode('utf-8')
    
    try:
        if counter < 5:
            leitura.append(float(payload))  # This will print the parsed JSON data

        if counter == 4:
            new_df = pd.DataFrame([leitura], columns=df.columns)
            df = pd.concat([df, new_df], ignore_index=True)
    
            counter = 0
            leitura = []
            
            print(df)
            return

        counter += 1

    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)

def on_disconnect(client, userdata, rc):
    print("Conexão perdida. Tentando reconectar...")
    print(userdata)
    print(rc)
    time.sleep(5)  # Aguarda 5 segundos antes de tentar reconectar
    client.reconnect()

def alert():
    while True:
        print('SERVER ON')
        time.sleep(20)

# Configurações do broker MQTT
mqtt_server = "mqtt.lab.cagepa.pb.gov.br"
mqtt_port = 2884
mqtt_topic = "ufpbcear/#"

# Criação de um cliente MQTT
client = mqtt.Client()
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set("ufpbcear", "Ufpb@C3ar23")

#thread = threading.Thread(target=alert)
#thread.daemon = True  # Define a thread como um daemon para encerrar junto com o programa principal
#thread.start()

# Loop de reconexão
while True:
    try:
        # Conecta-se ao broker MQTT
        client.connect(mqtt_server, port=mqtt_port)
        client.subscribe(mqtt_topic)
        # Mantém a conexão e lida com as mensagens recebidas
        client.loop_forever()
    except KeyboardInterrupt:
        # Encerra o loop se o usuário pressionar Ctrl+C
        break
