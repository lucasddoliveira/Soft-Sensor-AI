import paho.mqtt.client as mqtt
import json
from datetime import datetime, timedelta
import pytz
import mysql.connector
import time
import threading
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_TOPIC = os.getenv('MQTT_TOPIC')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_URI = os.getenv('MYSQL_URI')
MYSQL_TABLE = os.getenv('MYSQL_URI')
MYSQL_DATABASE = os.getenv('MYSQL_URI')

counter = 0
leitura = [0,0,0,0,0,0]
df = pd.DataFrame(columns=["DP_995796", "DP_564065", "DP_035903", "DP_012072", "DP_862640"])

def on_message(client, userdata, message):
    global counter, leitura, df  # Add these lines to declare counter, leitura, and df as global variables
    payload = message.payload.decode('utf-8')

    try:
        if counter < 5:
            if('DP_995796' in message.topic):
                leitura[1]= float(payload)  # This will print the parsed JSON data
            elif('DP_564065' in message.topic):
                leitura[2]= float(payload)
            elif('DP_035903' in message.topic):
                leitura[3]= float(payload)
            elif('DP_012072' in message.topic):
                leitura[4]= float(payload)
            elif('DP_862640' in message.topic):
                leitura[5]= float(payload)
            else:
                return

        if counter == 4:
            fuso_horario = pytz.timezone('America/Sao_Paulo')
            hora_atual = datetime.now(fuso_horario)
            leitura[0] = hora_atual

            cnx = mysql.connector.connect(
                host=MYSQL_URI,
                user=MYSQL_USERNAME,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )

            cursor = cnx.cursor()
            cursor.execute('INSERT INTO '+str(MYSQL_TABLE)+' (timestamp, DP_995796, DP_564065, DP_035903, DP_012072, DP_862640) VALUES (%s, %s, %s, %s, %s, %s)', leitura)


            cnx.commit()
            cursor.close()
            cnx.close()

            counter = 0
            leitura = [0,0,0,0,0,0]


            return

        counter += 1

    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)

def on_disconnect(client, userdata, rc):
    print("Conexão perdida. Tentando reconectar...")
    print(rc)
    time.sleep(5)  # Aguarda 5 segundos antes de tentar reconectar
    client.reconnect()

def alert():
    while True:
        print('SERVER ON')
        time.sleep(20)

# Criação de um cliente MQTT
client = mqtt.Client()
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

#thread = threading.Thread(target=alert)
#thread.daemon = True  # Define a thread como um daemon para encerrar junto com o programa principal
#thread.start()

# Loop de reconexão
while True:
    try:
        # Conecta-se ao broker MQTT
        client.connect(MQTT_BROKER, port= MQTT_PORT)
        client.subscribe(MQTT_TOPIC)
        # Mantém a conexão e lida com as mensagens recebidas
        client.loop_forever()
    except KeyboardInterrupt:
        # Encerra o loop se o usuário pressionar Ctrl+C
        break
