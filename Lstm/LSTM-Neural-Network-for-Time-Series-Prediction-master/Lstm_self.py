# -*- coding: UTF-8-*-
import datetime
import os
import pandas as pd
import numpy as np
from sklearn import metrics
from keras.models import Sequential
from keras.layers import LSTM
from keras.layers import Dense
from keras.layers import Dropout
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt
from Get_data import *
from os.path import dirname
from keras.callbacks import Callback
from keras.callbacks import ModelCheckpoint


plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
pd.set_option('mode.chained_assignment', None)

# 设置参数
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
# Y_train = Y
# X_train = X

# 获取1/4测试数据
# X_test, Y_test, point_temp = get_test_data_frome_TB(step, loction)


X_=[]
Y_=[]
arr=random_int_list(step, len(X), 100)
for i in  range(step,len(X)):
    if i not in arr:
        X_.append(X[i])
        Y_.append(Y[i])
X_train=np.concatenate(X_)
Y_train=np.concatenate(Y_)
X_train = np.reshape(np.array(X_train)[np.newaxis, :], (len(X_train) // step, step,len(X_train[0])))
Y_train=  np.array(Y_train)
print(X_train.shape)
print(Y_train.shape)

#随机选取几个测试
X_=[]
Y_=[]
for i in arr:
    X_.append(X[i])
    Y_.append(Y[i])
X_test=np.concatenate(X_)
Y_test=np.concatenate(Y_)
X_test = np.reshape(np.array(X_test)[np.newaxis, :], (len(X_test) // step, step,len(X_test[0])))
Y_tes=np.array(Y_test)
print(X_test.shape)
print(Y_test.shape)




# 建模
model = Sequential()
model.add(LSTM(Number_of_neurons,input_shape=(X_train.shape[1], X_train.shape[-1]),return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(Number_of_neurons, return_sequences=True))
model.add(Dropout(0.2))
model.add(LSTM(Number_of_neurons))
model.add(Dropout(0.2))
model.add(Dense(1))




model.compile(loss='mse', optimizer='rmsprop')
# model.save('model/lstm_model_best.h5')
filepath = 'model/lstm_model_9_best.h5'
callbacks = [ModelCheckpoint(filepath, monitor='loss', verbose=1, save_best_only=True, mode='min')]
model.summary()
# 训练模型
history = model.fit(X_train, Y_train,
                    epochs=200,
                    batch_size=50,
                    validation_data=(X_test, Y_test),
                    callbacks=callbacks)
loss = history.history['loss']
val_loss = history.history['val_loss']
plt.subplot(2, 1, 1)
plt.plot(range(len(loss)), val_loss, 'r-', label='测试集损失')
plt.legend(loc='best')

# 结果评估
model_pred = model.predict(X_test)
# 反归一化
# model_pred, Y_test, outdoor_temperature = con_normalization(model_pred, Y_test, point_temp)
model_pred, Y_test = con_normalization(model_pred, Y_test)
model_pred.reshape(len(model_pred))
Y_test.reshape(len(Y_test))
plt.subplot(2, 1, 2)
# plt.plot(outdoor_temperature, model_pred, 'b-', label='model_pred')
# plt.plot(outdoor_temperature, Y_test, 'r-', label='Y_test')
plt.plot( model_pred, 'b-', label='model_pred')
plt.plot(Y_test, 'r-', label='Y_test')
plt.legend(loc='upper left')
# plt.twinx()
# plt.plot(outdoor_temperature,'k:',label='outdoorT')
# plt.legend(loc='upper right')
path_picture = dirname(__file__) + '/picture'
folder_picture = os.path.exists(path_picture)
if not folder_picture:
    os.makedirs(path_picture)
name = '{}'.format(step)
plt.title(label='site:{}  method：LSTM step={} flow'.format(loction, step), y=-0.3)
file_name = '{path}/{name}.png'.format(path=path_picture, name=name)
plt.savefig(file_name, dpi=300)
plt.show()
