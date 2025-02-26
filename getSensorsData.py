import os
import json
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import threading
import pytz
import mysql.connector
import time
from AIModel.aiModel import SoftSensor
from dotenv import load_dotenv
import warnings
import numpy as np
warnings.filterwarnings('ignore')

load_dotenv()

MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_SUBSCRIBE_TOPIC = os.getenv('MQTT_SUBSCRIBE_TOPIC')
MQTT_PUBLISH_TOPIC1 = os.getenv('MQTT_PUBLISH_TOPIC1')
MQTT_PUBLISH_TOPIC2 = os.getenv('MQTT_PUBLISH_TOPIC2')
MQTT_PUBLISH_TOPIC3 = os.getenv('MQTT_PUBLISH_TOPIC3')
MQTT_PUBLISH_TOPIC4 = os.getenv('MQTT_PUBLISH_TOPIC4')
MQTT_PUBLISH_TOPIC5 = os.getenv('MQTT_PUBLISH_TOPIC5')
MQTT_PUBLISH_TOPIC6 = os.getenv('MQTT_PUBLISH_TOPIC6')
MQTT_PUBLISH_TOPIC7 = os.getenv('MQTT_PUBLISH_TOPIC7')
MQTT_PUBLISH_TOPIC8 = os.getenv('MQTT_PUBLISH_TOPIC8')
MQTT_PORT = int(os.getenv('MQTT_PORT'))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
MYSQL_URL = os.getenv('MYSQL_URL')
MYSQL_TABLE = os.getenv('MYSQL_TABLE')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

counter = 0
leitura = [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
ultima_leitura = [None, None, None, None, None, None, None, None, None]

def on_message(client, userdata, message):
    global counter, leitura, df, ultima_leitura
    payload = message.payload.decode('utf-8')
    
    try:
        if counter < 5:
            if 'DP_995796' in message.topic:
                leitura[1] = float(payload)
            elif 'DP_564065' in message.topic:
                leitura[2] = float(payload)
            elif 'DP_035903' in message.topic:
                leitura[3] = float(payload)
            elif 'DP_012072' in message.topic:
                leitura[4] = float(payload)
            elif 'DP_862640' in message.topic:
                leitura[5] = float(payload)
            else:
                return

        if counter == 4 and all(value != 0 for value in leitura[1:6]):
            # Verificar se as leituras dos sensores são iguais à última leitura válida
            if all(leitura[i] == ultima_leitura[i] for i in range(1, 6)):
                print(leitura[1:6])
                print(ultima_leitura[1:6])
                print('Leitura duplicada, ignorando pacote.')

                counter = 0
                leitura = [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                return
            
            fuso_horario = pytz.timezone('America/Sao_Paulo')
            leitura[0] = datetime.now(fuso_horario).replace(tzinfo=None)

            softSensorValue = SoftSensor(leitura)
            print('LSTM: ' + str(softSensorValue[0]) + ' | MLP: ' + str(softSensorValue[1]) + ' | CNN: ' + str(softSensorValue[2]) + ' | AUTO: ' + str(softSensorValue[3]))
            
            leitura[6] = softSensorValue[0]  # LSTM
            leitura[7] = softSensorValue[1]  # MLP
            leitura[8] = softSensorValue[2]  # CNN
            leitura[9] = softSensorValue[3][0][0]  # AUTO
            leitura[10] = softSensorValue[3][0][1]  # AUTO
            leitura[11] = softSensorValue[3][0][2]  # AUTO
            leitura[12] = softSensorValue[3][0][3]  # AUTO

            if np.isnan(leitura[7]):
                        leitura[7] = leitura[6]

            print(leitura)
            
            cnx = mysql.connector.connect(
                host=MYSQL_URL,
                user=MYSQL_USERNAME,
                password=MYSQL_PASSWORD,
                database=MYSQL_DATABASE
            )

            cursor = cnx.cursor()
            cursor.execute('INSERT INTO ' + str(MYSQL_TABLE) + ' (timestamp, DP_995796, DP_564065, DP_035903, DP_012072, DP_862640, LSTMValue, MLPValue, CNNValue, AENivel, AEPressao, AEPressaoRecalque, AEVazao ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)', leitura[0:-1])

            cnx.commit()
            cursor.close()
            cnx.close()
            
            publishSoftSensor(softSensorValue)
            
            # Atualizar a última leitura válida
            ultima_leitura = leitura.copy()
            
            counter = 0
            leitura = [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
            
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
    if(softSensorValue!=[505,505,505, 505]):
        client.publish(MQTT_PUBLISH_TOPIC1, softSensorValue[0])
        client.publish(MQTT_PUBLISH_TOPIC2, softSensorValue[1])
        client.publish(MQTT_PUBLISH_TOPIC3, softSensorValue[2])
        client.publish(MQTT_PUBLISH_TOPIC4, softSensorValue[3][0][0])
        client.publish(MQTT_PUBLISH_TOPIC5, softSensorValue[3][0][1])
        client.publish(MQTT_PUBLISH_TOPIC6, softSensorValue[3][0][2])
        client.publish(MQTT_PUBLISH_TOPIC7, softSensorValue[3][0][3])


client = mqtt.Client()
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

thread = threading.Thread(target=publishSoftSensor([505,505,505, 505]))
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
