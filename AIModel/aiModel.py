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

    def predict_flow(data, model_LSTM, model_MLP):
        # Predição LSTM
        data_LSTM, data_Y = preprocessar_LSTM(data)
        pred_LSTM = model_LSTM.predict(data_LSTM)
        predDenorm_LSTM = Denormalize(pred_LSTM, data_Y)

        # Predição MLP
        data_MLP, data_Y = preprocessar_LSTM(data)
        pred_MLP = model_MLP.predict(data_MLP)
        predDenorm_MLP = Denormalize(pred_MLP, data_Y)

        return predDenorm_LSTM, predDenorm_MLP

    def preprocessar_LSTM(data):

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

        return Xn, data_Y
    
    def preprocessar_MLP(data):

        data.rename(columns={'timestamp':'Tempo', 'DP_564065':'nivel','DP_862640': 'pressao', 'DP_012072':'vazao_recalque','DP_035903':'pressao_recalque', 'DP_995796':'vazao_(t)', 'softSensorValue':'softSensorValue'}, inplace=True)
        data.index = data['Tempo']
        data['Tempo'] = pd.to_datetime(data['Tempo'])
        data.drop(columns = ['Tempo'],inplace=True)
        data.drop(columns = ['softSensorValue'],inplace=True)
        # Mascara Booleana para variavel vazão_recalque
        # vazão-recalque -> ativação da bomba p aumentar o nivel de agua no reservatorio (1-ligada 0-desligada)
        data.loc[(data['vazao_recalque'] < 0), 'vazao_recalque'] = 0
        data.loc[(data['vazao_recalque'] > 0), 'vazao_recalque'] = 1

        x1 = data
        x2 = x1.shift(1).rename(columns={'nivel':'nivel(t-1)','pressao':'pressao(t-1)','vazao_recalque':'vazao_recalque(t-1)', 'pressao_recalque':'pressao_recalque(t-1)','vazao_(t)':'vazao_(t-1)'})
        x3 = x1.shift(2).rename(columns={'nivel':'nivel(t-2)','pressao':'pressao(t-2)','vazao_recalque':'vazao_recalque(t-2)', 'pressao_recalque':'pressao_recalque(t-2)','vazao_(t)':'vazao_(t-2)'})
        x4 = x1.shift(3).rename(columns={'nivel':'nivel(t-3)','pressao':'pressao(t-3)','vazao_recalque':'vazao_recalque(t-3)', 'pressao_recalque':'pressao_recalque(t-3)','vazao_(t)':'vazao_(t-3)'})
        x5 = x1.shift(4).rename(columns={'nivel':'nivel(t-4)','pressao':'pressao(t-4)','vazao_recalque':'vazao_recalque(t-4)', 'pressao_recalque':'pressao_recalque(t-4)','vazao_(t)':'vazao_(t-4)'})

        y1 = x1.shift(-1).rename(columns={'nivel':'nivel(t+1)','pressao':'pressao(t+1)','vazao_recalque':'vazao_recalque(t+1)', 'pressao_recalque':'pressao_recalque(t+1)','vazao_(t)':'vazao_(t+1)'})
        
        dataShift = pd.concat([x1,x2,x3,x4,x5,y1],axis=1).dropna(axis=0)

        data_X = dataShift.iloc[:, 0:29].drop(dataShift.columns[[4]], axis=1)
        data_Y = dataShift.iloc[:, 29]

        Xn = Normalize(data_X)
        return Xn, data_Y

    #Carregar modelo
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Modelo LSTM
    model_path_LSTM = os.path.join(script_dir, './modeloLSTM20neurons.h5')
    model_LSTM = tf.keras.models.load_model(model_path_LSTM)

    # Modelo MLP
    model_path_MLP = os.path.join(script_dir, './modeloMLP16neurons_v1.h5')
    model_MLP = tf.keras.models.load_model(model_path_MLP)

    # Leitura dos dados do SQL
    df = pd.read_sql(query, connection)
    new_row_df = pd.DataFrame([inputData], columns=['timestamp', 'DP_995796','DP_564065','DP_012072','DP_035903','DP_862640', 'softSensorValue'])
    df = pd.concat([new_row_df, df], ignore_index=True)
    
    #Predição
    pred_LSTM, pred_MLP = predict_flow(df, model_LSTM, model_MLP)

    connection.close()

    softSensorLSTM = round(float(pred_LSTM[0][0]), 2)
    softSensorMLP = round(float(pred_MLP[0][0]), 2)

    #print(f"Previsão da vazão: {softSensorValue}")
    return softSensorLSTM, softSensorMLP
