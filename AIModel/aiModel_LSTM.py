import pandas as pd
import numpy as np
import mysql.connector
from dotenv import load_dotenv
import os
from silence_tensorflow import silence_tensorflow
silence_tensorflow()
import tensorflow as tf

def SoftSensor(inputData):
    load_dotenv()

    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
    MYSQL_USERNAME = os.getenv('MYSQL_USERNAME')
    MYSQL_URL = os.getenv('MYSQL_URL')
    MYSQL_TABLE = os.getenv('MYSQL_TABLE')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

    connection= mysql.connector.connect(
                    host=MYSQL_URL,
                    user=MYSQL_USERNAME,
                    password=MYSQL_PASSWORD,
                    database=MYSQL_DATABASE
                )

    query = "SELECT * FROM " + str(MYSQL_TABLE) + " ORDER BY timestamp DESC LIMIT 10"


    def transform_data(window_size, X, y):
        '''
        Inputs:
        X -> array de dados de entrada
        y -> array de dados de saída
        Outputs:
        X_transformed -> array de dados de entrada transformado com window_size
        y_transformed -> array de dados de saída transformado com window_size
        '''
        X_transformed = []
        y_transformed = []
        for i in range(len(X) - window_size):
            X_transformed.append(X[i:i+window_size])
            y_transformed.append(y[i+window_size])
        X_transformed = np.array(X_transformed)
        y_transformed = np.array(y_transformed)
        return X_transformed, y_transformed

    def Normalize(data):
        data_max = data.max()
        data_min = data.min()
        return (data-data_min)/(data_max-data_min)

    def Denormalize(data_normalized, data):
        data_max = data.max()
        data_min = data.min()
        return data_normalized*(data_max-data_min) + data_min

    def predict_flow(data):

        data_processada, data_Y = preprocessar_dados(data)
        previsao = model.predict(data_processada)
        previsao_d = Denormalize(previsao, data_Y)

        return previsao_d

    def preprocessar_dados(data):

        data.rename(columns={'timestamp':'Tempo', 'DP_564065':'nivel','DP_862640': 'pressao', 'DP_012072':'vazao_recalque','DP_035903':'pressao_recalque', 'DP_995796':'vazao_(t)', 'softSensorValue':'softSensorValue'}, inplace=True)
        data.index = data['Tempo']
        data['Tempo'] = pd.to_datetime(data['Tempo'])
        #data.drop(columns = ['Tempo','Unnamed: 0', 'vazao'],inplace=True)
        data.drop(columns = ['Tempo'],inplace=True)
        data.drop(columns = ['softSensorValue'],inplace=True)
        # Mascara Booleana para variavel vazão_recalque
        # vazão-recalque -> ativação da bomba p aumentar o nivel de agua no reservatorio (1-ligada 0-desligada)
        data.loc[(data['vazao_recalque'] < 0), 'vazao_recalque'] = 0
        data.loc[(data['vazao_recalque'] > 0), 'vazao_recalque'] = 1
        data_X = data.iloc[:, 0:5]
        data_Y = data.iloc[:, 4]

        window_size = 10
        data_X, data_Y = transform_data(window_size,data_X, data_Y)

        Xn = Normalize(data_X)
        #Yn = Normalize(data_Y)
        #Yn = np.array(Yn).reshape(-1, 1)

        features_normalizadas = Xn

        return features_normalizadas, data_Y

    #Carregar modelo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(script_dir, './modeloLSTM20neurons.h5')
    model = tf.keras.models.load_model(model_path)

    # Leitura dos dados do SQL
    df = pd.read_sql(query, connection)
    new_row_df = pd.DataFrame([inputData], columns=['timestamp', 'DP_995796','DP_564065','DP_012072','DP_035903','DP_862640', 'softSensorValue'])
    df = pd.concat([new_row_df, df], ignore_index=True)
    
    previsao = predict_flow(df)

    connection.close()

    softSensorValue = round(float(previsao[0][0]), 2)

    #print(f"Previsão da vazão: {softSensorValue}")
    return softSensorValue
