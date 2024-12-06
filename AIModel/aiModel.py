import pandas as pd
import numpy as np
import mysql.connector
from dotenv import load_dotenv
import os
from silence_tensorflow import silence_tensorflow
silence_tensorflow()
import tensorflow as tf
from .teda_algo import TEDADetect
import requests
# token do bot gerado pelo BotFather (telegram)
bot_token = "7160209549:AAEgSGm0T-XRbpPwHVROsTFHQNxE4lV8KOc"

# ID do chat para onde as mensagens serão enviadas
chat_id = "6412205514"

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
    
    # função utilizada no AutoEncoder
    def transform_X(window_size, X):
        '''
        Inputs:
        X -> array de dados de entrada
        Outputs:
        X_transformed -> array de dados de entrada transformado com window_size
        '''
        X_transformed = []
        for i in range(len(X) - window_size):
            X_transformed.append(X[i:i+window_size])
        X_transformed = np.array(X_transformed)
        return X_transformed

    def Normalize(data):
        data_max = data.max()
        data_min = data.min()
        return (data-data_min)/(data_max-data_min)

    def Denormalize(data_normalized, data):
        data_max = data.max()
        data_min = data.min()
        return data_normalized*(data_max-data_min) + data_min

    def predict_flow(data, model_LSTM, model_CNN, model_MLP, modelo_AUTO):
        # Predição LSTM
        data_LSTM, data_Y = preprocessar_LSTM(data)
        pred_LSTM = model_LSTM.predict(data_LSTM)
        predDenorm_LSTM = Denormalize(pred_LSTM, data_Y)

        # Predição CNN
        data_CNN, data_Y = preprocessar_LSTM(data)
        pred_CNN = model_CNN.predict(data_CNN)
        predDenorm_CNN = Denormalize(pred_CNN, data_Y)

        # Predição MLP
        data_MLP, data_Y = preprocessar_MLP(data)
        pred_MLP = model_MLP.predict(data_MLP)
        predDenorm_MLP = Denormalize(pred_MLP, data_Y)

        # Predição AUTOE
        data_Xn, data_AUTO = preprocessar_AUTO(data)
        pred_AUTO = model_AUTO.predict(data_Xn)
        media_Xn = np.mean(data_Xn, axis=1)
        media_AUTO = np.mean(pred_AUTO, axis=1)
        predDenorm_AUTO = Denormalize(media_AUTO, data_AUTO)


        return predDenorm_LSTM, predDenorm_CNN, predDenorm_MLP, predDenorm_AUTO

    def preprocessar_LSTM(input_data):
        
        data = input_data.copy()
        
        data.rename(columns={'timestamp':'Tempo', 'DP_564065':'nivel','DP_862640': 'pressao', 'DP_012072':'vazao_recalque','DP_035903':'pressao_recalque', 'DP_995796':'vazao_(t)', 'LSTMValue':'LSTMValue','MLPValue':'MLPValue','AENivel':'AENivel', 'AEPressao':'AEPressao', 'AEVazaoRecalque':'AEVazaoRecalque', 'AEPressaoRecalque': 'AEPressaoRecalque', 'AEVazao': 'AEVazao'}, inplace=True)
        data.index = data['Tempo']
        data['Tempo'] = pd.to_datetime(data['Tempo'])
        #data.drop(columns = ['Tempo','Unnamed: 0', 'vazao'],inplace=True)
        data.drop(columns = ['Tempo'],inplace=True)
        data.drop(columns = ['LSTMValue'],inplace=True)
        data.drop(columns = ['MLPValue'],inplace=True)
        data.drop(columns = ['CNNValue'],inplace=True)
        data.drop(columns = ['AENivel'],inplace=True)
        data.drop(columns = ['AEPressao'],inplace=True)
        data.drop(columns = ['AEVazaoRecalque'],inplace=True)
        data.drop(columns = ['AEPressaoRecalque'],inplace=True)
        data.drop(columns = ['AEVazao'],inplace=True)
        # Mascara Booleana para variavel vazão_recalque
        # vazão-recalque -> ativação da bomba p aumentar o nivel de agua no reservatorio (1-ligada 0-desligada)
        data.loc[(data['vazao_recalque'] < 0), 'vazao_recalque'] = 0
        data.loc[(data['vazao_recalque'] > 0), 'vazao_recalque'] = 1
        # determina ordem que as features devem estar dispostas
        data = data[['nivel', 'pressao', 'vazao_recalque', 'pressao_recalque', 'vazao_(t)']]
        data_X = data.iloc[:, 0:5]
        data_Y = data.iloc[:, 4]

        window_size = 10
        data_X, data_Y = transform_data(window_size,data_X, data_Y)

        Xn = Normalize(data_X)
        #Yn = Normalize(data_Y)
        #Yn = np.array(Yn).reshape(-1, 1)

        return Xn, data_Y
    
    def preprocessar_MLP(input_data1):
        
        data = input_data1.copy()
        
        data.rename(columns={'timestamp':'Tempo', 'DP_564065':'nivel','DP_862640': 'pressao', 'DP_012072':'vazao_recalque','DP_035903':'pressao_recalque', 'DP_995796':'vazao_(t)', 'LSTMValue':'LSTMValue','MLPValue':'MLPValue','AENivel':'AENivel', 'AEPressao':'AEPressao', 'AEVazaoRecalque':'AEVazaoRecalque', 'AEPressaoRecalque': 'AEPressaoRecalque', 'AEVazao': 'AEVazao'}, inplace=True)
        data.index = data['Tempo']
        data['Tempo'] = pd.to_datetime(data['Tempo'])
        data.drop(columns = ['Tempo'],inplace=True)
        data.drop(columns = ['LSTMValue'],inplace=True)
        data.drop(columns = ['MLPValue'],inplace=True)
        data.drop(columns = ['CNNValue'],inplace=True)
        data.drop(columns = ['AENivel'],inplace=True)
        data.drop(columns = ['AEPressao'],inplace=True)
        data.drop(columns = ['AEVazaoRecalque'],inplace=True)
        data.drop(columns = ['AEPressaoRecalque'],inplace=True)
        data.drop(columns = ['AEVazao'],inplace=True)
        # Mascara Booleana para variavel vazão_recalque
        # vazão-recalque -> ativação da bomba p aumentar o nivel de agua no reservatorio (1-ligada 0-desligada)
        data.loc[(data['vazao_recalque'] < 0), 'vazao_recalque'] = 0
        data.loc[(data['vazao_recalque'] > 0), 'vazao_recalque'] = 1
        data = data[['nivel', 'pressao', 'vazao_recalque', 'pressao_recalque', 'vazao_(t)']]

        shifts = 5
        dfs = []

        for i in range(-1, shifts + 1):
            shifted_df = data.shift(i)
            if i>0:
                renamed_columns = {col: f"{col}(t-{i})" for col in shifted_df.columns}
            elif(i==0):
                renamed_columns = {col: f"{col}" for col in shifted_df.columns}
            else:
                renamed_columns = {col: f"{col}(t+{-i})" for col in shifted_df.columns}
            shifted_df = shifted_df.rename(columns=renamed_columns)
            dfs.append(shifted_df)

        y1, x0, x1, x2, x3, x4, x5 = dfs

        dataShift = pd.concat([x0,x1,x2,x3,x4,x5,y1],axis=1).dropna(axis=0)

        data_X = dataShift.iloc[:, 0:30]
        data_Y = dataShift.iloc[:, 34]

        Xn = Normalize(data_X)
        return Xn, data_Y
    
    def preprocessar_AUTO(input_data2):
        data = input_data2.copy()

        data.rename(columns={'timestamp':'Tempo', 'DP_564065':'nivel','DP_862640': 'pressao', 'DP_012072':'vazao_recalque','DP_035903':'pressao_recalque', 'DP_995796':'vazao', 'LSTMValue':'LSTMValue','MLPValue':'MLPValue','AENivel':'AENivel', 'AEPressao':'AEPressao', 'AEVazaoRecalque':'AEVazaoRecalque', 'AEPressaoRecalque': 'AEPressaoRecalque', 'AEVazao': 'AEVazao'}, inplace=True)
        data.index = data['Tempo']
        data['Tempo'] = pd.to_datetime(data['Tempo'])
        # Remover colunas irrelevantes
        cols_to_drop = ['Tempo', 'LSTMValue', 'MLPValue', 'CNNValue', 'AENivel',
                    'AEPressao', 'AEVazaoRecalque', 'AEPressaoRecalque',
                    'AEVazao', 'vazao_recalque']
        data.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        
        # assegura a ordem das features
        data = data[['nivel', 'pressao', 'pressao_recalque', 'vazao']]

        data_AUTO = data.iloc[:, 0:4]
        window_size = 10
        data_AUTO = transform_X(window_size,data_AUTO)

        data_Xn = Normalize(data_AUTO)

        return data_Xn, data_AUTO
    
    def enviar_alerta_telegram(mensagem):
        """
        Envia um alerta para o Telegram.
        """
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {"chat_id": chat_id, "text": mensagem}
        response = requests.post(url, data=data)

        
    def processar_outliers(df):
        """
        Processa os dados, identifica outliers e envia alertas se >10% por hora.
        """
        print(type(df))
        # Renomear a coluna para garantir consistência
        df.rename(columns={'DP_995796': 'vazao'}, inplace=True)
        print('A')
        # Garante o tratamento como dado de tempo
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        print('B')
        # Instanciar o objeto TEDADetect
        teda = TEDADetect()
        print('C')
        # Adicionar uma nova coluna 'is_outlier' com os resultados
        df['is_outlier'] = df['vazao'].apply(lambda x: teda.run([x], 2))  # Chama o método online
        print('D')
        # Agrupar os dados por hora
        df['hour'] = df['timestamp'].dt.floor('H')
        hourly_stats = df.groupby('hour').agg(
            total=('is_outlier', 'size'),
            outliers=('is_outlier', 'sum')
        )

        print('E')
        # Calcular o percentual de outliers
        hourly_stats['outlier_percentage'] = (hourly_stats['outliers'] / hourly_stats['total']) * 100

        # Verificar horas com mais de 10% de outliers e enviar alertas
        for index, row in hourly_stats.iterrows():
            if row['outlier_percentage'] > 10:
                mensagem = (
                    f"🚨 Alerta de Outliers 🚨\n"
                    f"Na hora {index}, {row['outlier_percentage']:.2f}% dos dados foram classificados como outliers.\n"
                    f"📊 Total de dados: {row['total']}, Outliers: {row['outliers']}."
                )
                enviar_alerta_telegram(mensagem)

    #Carregar modelo
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(tf.__version__, tf.keras.__version__)
    # Modelo LSTM
    model_path_LSTM = os.path.join(script_dir, 'modeloLSTM20neurons.h5')
    model_LSTM = tf.keras.models.load_model(model_path_LSTM)

    # Modelo CNN
    model_path_CNN = os.path.join(script_dir, 'modeloCNN8neurons_v1.h5')
    model_CNN = tf.keras.models.load_model(model_path_CNN)

    # Modelo MLP
    model_path_MLP = os.path.join(script_dir, 'modeloMLP30neurons_v4.h5')
    model_MLP = tf.keras.models.load_model(model_path_MLP)

    # Modelo AUTO
    model_path_AUTO = os.path.join(script_dir, 'best_modelAE_combination_3.h5')
    model_AUTO = tf.keras.models.load_model(model_path_AUTO)

    # Leitura dos dados do SQL
    df = pd.read_sql(query, connection)
    new_row_df = pd.DataFrame([inputData], columns=['timestamp', 'DP_995796','DP_564065','DP_012072','DP_035903','DP_862640', 'LSTMValue', 'MLPValue', 'CNNValue', 'AENivel', 'AEPressao', 'AEVazaoRecalque', 'AEPressaoRecalque', 'AEVazao'])
    df = pd.concat([new_row_df, df], ignore_index=True)
    connection.close()

    #Predição
    pred_LSTM, pred_CNN, pred_MLP, pred_AUTO = predict_flow(df, model_LSTM, model_CNN, model_MLP, model_AUTO)
    softSensorLSTM = round(float(pred_LSTM[0][0]), 3)
    softSensorMLP  = round(float(pred_MLP[0][0]), 3)
    softSensorCNN  = round(float(pred_CNN[0][0]), 3)
    softSensorAUTO  = np.round(pred_AUTO.astype(float),3)
    
    processar_outliers(df)

    #print(f"Previsão da vazão: {softSensorValue}")
    return [softSensorLSTM, softSensorMLP, softSensorCNN, softSensorAUTO]
