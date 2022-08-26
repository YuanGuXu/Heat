#-*- coding: UTF-8-*-
import datetime
import sys
import numpy as np
import pandas as pd
pd.set_option('mode.chained_assignment', None)
def random_int_list(start, stop, length):
    import random
    start, stop = (int(start), int(stop)) if start <= stop else (int(stop), int(start))
    length = int(abs(length)) if length else 0
    random_list = []
    for i in range(length):
        random_list.append(random.randint(start, stop))
    return random_list

def get_data(data,step,location):
    """
    :param data:输入数据dataframe
    :param step: 用于Lstm的步长
    :return: 训练集
    """
    data.fillna(value=0,inplace=True)
    data_cols=['time','S_side_Temp_{}'.format(location),'Inndoor_Temp_{}'.format(location),'outdoor_temperature','outdoor_lux','S_side_flow_{}'.format(location)]
    data=data[data_cols]
    data = data.dropna(axis=0, how='any')
    data = normalization(data, location)
    print(data.columns)
    data['time'] = pd.to_datetime(data['time'])
    #取标准差
    std_Inndoor_Temp=data['Inndoor_Temp_{}'.format(location)].rolling(step).std().tolist()
    std_outdoor_temperature=data['outdoor_temperature'].rolling(step).std().tolist()
    std_outdoor_lux = data['outdoor_lux'].rolling(step).std().tolist()
    std_S_side_flow = data['S_side_flow_{}'.format(location)].rolling(step).std().tolist()
    #取方差
    var_Inndoor_Temp = data['Inndoor_Temp_{}'.format(location)].rolling(step).var().tolist()
    var_outdoor_temperature = data['outdoor_temperature'].rolling(step).var().tolist()
    var_outdoor_lux = data['outdoor_lux'].rolling(step).var().tolist()
    var_S_side_flow = data['S_side_flow_{}'.format(location)].rolling(step).var().tolist()
    #取均值
    mean_Inndoor_Temp = data['Inndoor_Temp_{}'.format(location)].rolling(step).mean().tolist()
    mean_outdoor_temperature = data['outdoor_temperature'].rolling(step).mean().tolist()
    mean_outdoor_lux = data['outdoor_lux'].rolling(step).mean().tolist()
    mean_S_side_flow = data['S_side_flow_{}'.format(location)].rolling(step).mean().tolist()
    #取最小值
    min_Inndoor_Temp = data['Inndoor_Temp_{}'.format(location)].rolling(step).min().tolist()
    min_outdoor_temperature = data['outdoor_temperature'].rolling(step).min().tolist()
    min_outdoor_lux = data['outdoor_lux'].rolling(step).min().tolist()
    min_S_side_flow = data['S_side_flow_{}'.format(location)].rolling(step).min().tolist()
    #取最大值
    max_Inndoor_Temp = data['Inndoor_Temp_{}'.format(location)].rolling(step).max().tolist()
    max_outdoor_temperature = data['outdoor_temperature'].rolling(step).max().tolist()
    max_outdoor_lux = data['outdoor_lux'].rolling(step).max().tolist()
    max_S_side_flow = data['S_side_flow_{}'.format(location)].rolling(step).max().tolist()
    print(var_Inndoor_Temp)
    X=[]
    Y=[]
    outdoorT=[]
    count=0
    for i in range(step,len(data)-step-1):
        if data['time'].iloc[i]+datetime.timedelta(hours=step+1)==data['time'].iloc[i+step+1]:
            for j in range(i,i+step):
                X.append([data['Inndoor_Temp_{}'.format(location)].iloc[j],data['outdoor_temperature'].iloc[j],data['outdoor_lux'].iloc[j],data['S_side_flow_{}'.format(location)].iloc[j]])
                # X.append([data['Inndoor_Temp_{}'.format(location)].iloc[j],data['outdoor_temperature'].iloc[j],data['S_side_flow_{}'.format(location)].iloc[j]])

                # X.append([data['Inndoor_Temp_{}'.format(location)].iloc[j],data['outdoor_temperature'].iloc[j],data['outdoor_lux'].iloc[j],data['S_side_flow_{}'.format(location)].iloc[j],
                #                               std_Inndoor_Temp[i],
                #                               std_outdoor_temperature[i],
                #                               std_outdoor_lux[i],
                #                               std_S_side_flow[i],
                #                             var_Inndoor_Temp[i] ,
                #                             var_outdoor_temperature[i],
                #                             var_outdoor_lux[i],
                #                             var_S_side_flow[i],
                #                         mean_Inndoor_Temp[i],
                #                         mean_outdoor_temperature[i],
                #                         mean_outdoor_lux[i],
                #                         mean_S_side_flow[i],
                #                     min_Inndoor_Temp[i],
                #                     min_outdoor_temperature[i],
                #                     min_outdoor_lux[i],
                #                     min_S_side_flow[i],
                #                 max_Inndoor_Temp[i],
                #                 max_outdoor_temperature[i],
                #                 max_outdoor_lux[i],
                #                 max_S_side_flow[i]
                # ])

            Y.append([data['S_side_Temp_{}'.format(location)].iloc[i+step]])
            outdoorT.append(data['outdoor_temperature'].iloc[i+step])
        else:
            count+=1
    print("count:",count)
    X=np.reshape(X,(len(X)//step,step,len(X[0])))
    Y=np.array(Y)
    print("X.shape",X.shape)
    print("Y.shape",Y.shape)
    long=step
    return X,Y,outdoorT,long

def random_test_list(start,end,length,step,data,location):
    """
    :param start:采样区间的左端点
    :param end: 采样区间的右端点
    :param length: 样本个数
    :param step: 用于Lstm的步长，和训练集对应
    :param data: 输入数据dataframe
    :return: 测试集
    """
    import random
    X_test=[]
    Y_test=[]
    outdoor_temperature=[]
    arr=random.sample(range(start, end), length)
    for i in arr:
        if i<end-step:
            for j in range(i, i + step):
                X_test.append([data['Inndoor_Temp_{}'.format(location)].iloc[j], data['outdoor_temperature'].iloc[j]])
            Y_test.append([data['S_side_flow_{}'.format(location)].iloc[i + step]])
            outdoor_temperature.append(data['outdoor_temperature'].iloc[j])
    X_test = np.reshape(X_test, (len(X_test) // step, step, 3))
    Y_test = np.array(Y_test)
    print("X.shape", X_test.shape)
    print("Y.shape", Y_test.shape)
    return X_test,Y_test,outdoor_temperature
def normalization(df,location):#归一化
    cols=['time','S_side_Temp_{}'.format(location),'Inndoor_Temp_{}'.format(location),'outdoor_temperature','outdoor_lux','S_side_flow_{}'.format(location)]
    df=df[cols]

    Max_outdoorT=20.0
    Min_outdoorT=-20.0
    Max_indoorT=30.0
    Min_indoorT=10.0
    Max_S_side=60.0
    Min_S_side=10.0
    Max_outdoor_lux = 100000
    Min_outdoor_lux = 0
    Max_S_side_flow=60
    Min_S_side_flow=0
    df['S_side_Temp_{}'.format(location)] = (df['S_side_Temp_{}'.format(location)]-Min_S_side)/(Max_S_side-Min_S_side)
    df['Inndoor_Temp_{}'.format(location)] = (df['Inndoor_Temp_{}'.format(location)] - Min_indoorT) / (Max_indoorT - Min_indoorT)
    df['outdoor_temperature']  = (df['outdoor_temperature'] - Min_outdoorT) / (Max_outdoorT - Min_outdoorT)
    df['outdoor_lux'] = (df['outdoor_lux'] - Min_outdoor_lux) / (Max_outdoor_lux - Min_outdoor_lux)
    df['S_side_flow_{}'.format(location)] = (df['S_side_flow_{}'.format(location)] - Min_S_side_flow) / (Max_S_side_flow - Min_S_side_flow)
    return df

def con_normalization(model_pred,Y_test):#反归一
    Max_outdoorT = 20.0
    Min_outdoorT = -20.0
    Max_indoorT = 30.0
    Min_indoorT = 10.0

    Max_S_side = 60.0
    Min_S_side = 10.0

    outdoorT=[]
    for i in range(0,len(model_pred)):
        model_pred[i]=model_pred[i]*(Max_S_side-Min_S_side)+Min_S_side
        Y_test[i]=Y_test[i]*(Max_S_side-Min_S_side)+Min_S_side
        # outdoorT.append(outdoor_temperature[i]*(Max_outdoorT-Min_outdoorT)+Min_outdoorT)
        outdoorT=Y_test
    return model_pred,Y_test


def merg():
    df1=pd.read_csv('C:/Users/nameless/Desktop/S1.csv')
    df1['time']=df1['time'].astype('datetime64')
    df2=pd.read_csv('C:/Users/nameless/Desktop/S2.csv')
    df2['hour']=df2['collection_time'].apply(lambda x:datetime.datetime.strptime(x[:13],"%Y-%m-%d %H"))
    df2['hour']=df2['hour'].astype('datetime64')
    df3=pd.read_csv('C:/Users/nameless/Desktop/S3.csv')
    df3['collection_time']=df3['collection_time'].astype('datetime64')
    df2=df2.groupby('hour').mean()
    df_new=pd.merge(df1,df2,left_on='time',right_on='hour',how='inner')
    df_final=pd.merge(df_new,df3,left_on='time',right_on='collection_time')
    df_final.dropna(axis=0,how='any')
    # df_final.to_csv('C:/Users/nameless/Desktop/final_ALL.csv')
    return df_final
def get_test_data_frome_TB(step,location):
    """
    :param step:LSTM的步长
    :param location: 'Z','G','D'
    :return: 根据四分位表制作测试集（每个样本中各步长数据相同）
    """
    data=pd.read_csv('C:/Users/nameless/Desktop/heat_file_025_{}_flow.csv'.format(location))
    round(data, 1)
    Max_outdoorT = 20.0
    Min_outdoorT = -20.0
    Max_indoorT = 30.0
    Min_indoorT = 10.0

    Max_S_side = 60.0
    Min_S_side = 10.0
    Max_outdoor_lux=100000
    Min_outdoor_lux=0
    Max_S_side_flow=50
    Min_S_side_flow=0
    data['point_temp']=(data['point_temp'] - Min_outdoorT) / (Max_outdoorT - Min_outdoorT)
    data['S_side_Temp_{}'.format(location)]=(data['S_side_Temp_{}'.format(location)] - Min_S_side) / (Max_S_side - Min_S_side)
    data['Inndoor_Temp_{}'.format(location)]=(data['Inndoor_Temp_{}'.format(location)] - Min_indoorT) / (Max_indoorT - Min_indoorT)
    data['outdoor_lux']=(data['outdoor_lux']-Min_outdoor_lux)/(Max_outdoor_lux-Min_outdoor_lux)
    data['S_side_flow_{}'.format(location)] = (data['S_side_flow_{}'.format(location)] - Min_S_side_flow) / (Max_S_side_flow - Min_S_side_flow)
    S_side_Temp=data['S_side_Temp_{}'.format(location)].tolist()
    point_temp=data['point_temp'].tolist()
    Inndoor_Temp=data['Inndoor_Temp_{}'.format(location)].tolist()
    Outdoor_lux=data['outdoor_lux'].tolist()
    S_side_flow=data['S_side_flow_{}'.format(location)].tolist()
    X_test=[]
    Y_test_TB=[]
    for i in range(0, len(data)):
        for j in range(0, step):
            # X_test.append([Inndoor_Temp[i], point_temp[i],Outdoor_lux[i],S_side_flow[i]])
            X_test.append([Inndoor_Temp[i], point_temp[i],Outdoor_lux[i],S_side_flow[i]
                           #    ,0,0,0,0
                           #    ,0,0,0,0,
                           # Inndoor_Temp[i], point_temp[i],Outdoor_lux[i],S_side_flow[i],
                           # Inndoor_Temp[i], point_temp[i],Outdoor_lux[i],S_side_flow[i],
                           # Inndoor_Temp[i], point_temp[i],Outdoor_lux[i],S_side_flow[i]
                           ])

        Y_test_TB.append([S_side_Temp[i]])
    X_test = np.reshape(np.array(X_test)[np.newaxis, :], (len(X_test) // step, step,len(X_test[0])))
    Y_test_TB=np.array(Y_test_TB)
    return X_test,Y_test_TB,point_temp




def getdata_cnn():
    step =12
    Number_of_neurons = 32
    loction = 'Z'
    # 获取训练数据
    data = merg()
    cols = ['time', '二次侧中区供水温度', 'outdoor_temperature', 'outdoor_lux', 'indoor_temperature_xmly_{}'.format(loction),
            '二次侧中区流量']
    data = data[cols]
    new_cols = ['time', 'S_side_Temp_{}'.format(loction), 'outdoor_temperature', 'outdoor_lux',
                'Inndoor_Temp_{}'.format(loction), 'S_side_flow_{}'.format(loction)]
    data.columns = new_cols
    data['S_side_flow_{}'.format(loction)][data['S_side_flow_{}'.format(loction)]<0]=0
    data=data.loc[(data['S_side_Temp_{}'.format(loction)]<60) & (data['S_side_Temp_{}'.format(loction)]>10)]
    data = data.loc[data['time'] < '2022-04-01'].loc[data['time'] > '2021-11-01']
    data = data.loc[data['S_side_Temp_{}'.format(loction)] < 60].loc[data['S_side_Temp_{}'.format(loction)] > 10]
    data = data.loc[data['Inndoor_Temp_{}'.format(loction)] < 30].loc[data['Inndoor_Temp_{}'.format(loction)] > 10]
    data = data.loc[data['outdoor_temperature'] < 20].loc[data['outdoor_temperature'] > -20]
    dro=[]
    for i in range(0,len(data)):
        if data['S_side_flow_{}'.format(loction)].iloc[i]==0:
            dro.append(i)
    data.drop(index=dro, inplace=True)
    round(data,1)
    X, Y, outdoor_temperature, long = get_data(data, step, loction)
    return X, Y, outdoor_temperature, long
def data_TRAIN_TEST():
    """
    :return:输出训练、测试数据：其中测试数据随机选取且不包含于训练数据
    """
    # 设置参数
    step = 12
    Number_of_neurons = 32
    loction = 'Z'
    # 获取训练数据
    data = merg()
    cols = ['time', '二次侧中区供水温度', 'outdoor_temperature', 'outdoor_lux', 'indoor_temperature_xmly_{}'.format(loction),
            '二次侧中区流量']
    data = data[cols]
    new_cols = ['time', 'S_side_Temp_{}'.format(loction), 'outdoor_temperature', 'outdoor_lux',
                'Inndoor_Temp_{}'.format(loction), 'S_side_flow_{}'.format(loction)]
    data.columns = new_cols
    data['S_side_flow_{}'.format(loction)][data['S_side_flow_{}'.format(loction)] < 0] = 0
    data = data.loc[(data['S_side_Temp_{}'.format(loction)] < 60) & (data['S_side_Temp_{}'.format(loction)] > 10)]
    data = data.loc[data['time'] < '2022-04-01'].loc[data['time'] > '2021-11-01']
    data = data.loc[data['S_side_Temp_{}'.format(loction)] < 60].loc[data['S_side_Temp_{}'.format(loction)] > 10]
    data = data.loc[data['Inndoor_Temp_{}'.format(loction)] < 30].loc[data['Inndoor_Temp_{}'.format(loction)] > 10]
    data = data.loc[data['outdoor_temperature'] < 20].loc[data['outdoor_temperature'] > -20]
    dro = []
    for i in range(0, len(data)):
        if data['S_side_flow_{}'.format(loction)].iloc[i] == 0:
            dro.append(i)
    data.drop(index=dro, inplace=True)
    round(data, 1)
    X, Y, outdoor_temperature, long = get_data(data, step, loction)
    # Y_train = Y
    # X_train = X

    # 获取测试数据
    X_test, Y_test, point_temp = get_test_data_frome_TB(step, loction)

    X_ = []
    Y_ = []
    arr = random_int_list(step, len(X), 100)
    for i in range(step, len(X)):
        if i not in arr:
            X_.append(X[i])
            Y_.append(Y[i])
    X_train = np.concatenate(X_)
    Y_train = np.concatenate(Y_)
    X_train = np.reshape(np.array(X_train)[np.newaxis, :], (len(X_train) // step, step, len(X_train[0])))
    Y_train = np.array(Y_train)
    print(X_train.shape)
    print(Y_train.shape)

    # 随机选取几个测试
    X_ = []
    Y_ = []
    for i in arr:
        X_.append(X[i])
        Y_.append(Y[i])
    X_test = np.concatenate(X_)
    Y_test = np.concatenate(Y_)
    X_test = np.reshape(np.array(X_test)[np.newaxis, :], (len(X_test) // step, step, len(X_test[0])))
    Y_test = np.array(Y_test)
    print(X_test.shape)
    print(Y_test.shape)
    return X_train,Y_train,X_test,Y_test

