import paho.mqtt.client as mqtt
import time

# Configurações do broker MQTT
mqtt_server = "18.231.43.70"
mqtt_port = 1883
mqtt_topic = "softsensor/in"  # Tópico para publicar a mensagem

# Criação de um cliente MQTT
client = mqtt.Client()

# Conecta-se ao broker MQTT
client.connect(mqtt_server, port=mqtt_port)
client.loop_start()  # Inicia um loop de comunicação MQTT em um thread separado

# Função para enviar a mensagem a cada 5 segundos
def publish_message():
    while True:
        client.publish(mqtt_topic, "aiai")  # Publica a mensagem "aiai" no tópico especificado
        time.sleep(5)  # Aguarda 5 segundos

try:
    # Inicia a função para publicar mensagens
    publish_message()
except KeyboardInterrupt:
    # Encerra o programa se o usuário pressionar Ctrl+C
    pass
