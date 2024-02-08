import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import threading
import pytz
import mysql.connector
import time
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_SUBSCRIBE_TOPIC = os.getenv('MQTT_SUBSCRIBE_TOPIC')
MQTT_PUBLISH_TOPIC = os.getenv('MQTT_PUBLISH_TOPIC')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_URL = os.getenv('MYSQL_URL')
MYSQL_TABLE = os.getenv('MYSQL_TABLE')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

counter = 0
leitura = [0,0,0,0,0,0]

def on_message(client, userdata, message):
    global counter, leitura, df
    payload = message.payload.decode('utf-8')

    try:
        if counter < 5:
            if('DP_995796' in message.topic):
                leitura[1]= float(payload) 
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
            leitura[0] = datetime.now(fuso_horario)

            
            cnx = mysql.connector.connect(
                host=MYSQL_URL,
                user=MYSQL_USERNAME,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )

            cursor = cnx.cursor()
            cursor.execute('INSERT INTO '+str(MYSQL_TABLE)+' (timestamp, DP_995796, DP_564065, DP_035903, DP_012072, DP_862640) VALUES (%s, %s, %s, %s, %s, %s)', leitura)

            cnx.commit()
            cursor.close()
            cnx.close()
            

            #softSensorValue = SoftSensor(leitura[1],  leitura[2], leitura[3], leitura[4], leitura[5])
            publishSoftSensor(leitura[1]*2)

            counter = 0
            leitura = [0,0,0,0,0,0]
            
            return

        counter += 1

    except json.JSONDecodeError as e:
        print("JSON decoding error:", e)

def on_disconnect(client, userdata, rc):
    print("Conexão perdida. Tentando reconectar...")
    print(rc)
    time.sleep(5)  
    client.reconnect()

def publishSoftSensor(softSensorValue):
    global client
    client.publish(MQTT_PUBLISH_TOPIC, softSensorValue)

client = mqtt.Client()
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

thread = threading.Thread(target=publishSoftSensor(505))
thread.daemon = True 
thread.start()

while True:
    try:
        # Conecta-se ao broker MQTT
        client.connect(MQTT_BROKER, port= MQTT_PORT)
        client.subscribe(MQTT_SUBSCRIBE_TOPIC)
        # Mantém a conexão e lida com as mensagens recebidas
        client.loop_forever()
    except KeyboardInterrupt:
        # Encerra o loop se o usuário pressionar Ctrl+C
        break
