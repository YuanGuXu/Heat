# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import BPNN
from Get_data import *
import time
# step =1
# loction = 'Z'
# # 获取训练数据
# data = merg()
# cols = ['time', '二次侧中区供水温度', 'outdoor_temperature', 'outdoor_lux', 'indoor_temperature_xmly_{}'.format(loction),
#         '二次侧中区流量']
# data = data[cols]
# new_cols = ['time', 'S_side_Temp_{}'.format(loction), 'outdoor_temperature', 'outdoor_lux',
#             'Inndoor_Temp_{}'.format(loction), 'S_side_flow_{}'.format(loction)]
# data.columns = new_cols
# data['S_side_flow_{}'.format(loction)][data['S_side_flow_{}'.format(loction)]<0]=0
# data=data.loc[(data['S_side_Temp_{}'.format(loction)]<60) & (data['S_side_Temp_{}'.format(loction)]>10)]
# data = data.loc[data['time'] < '2022-04-01'].loc[data['time'] > '2021-11-01']
# data = data.loc[data['S_side_Temp_{}'.format(loction)] < 60].loc[data['S_side_Temp_{}'.format(loction)] > 10]
# data = data.loc[data['Inndoor_Temp_{}'.format(loction)] < 30].loc[data['Inndoor_Temp_{}'.format(loction)] > 10]
# data = data.loc[data['outdoor_temperature'] < 20].loc[data['outdoor_temperature'] > -20]
# round(data,1)
# X, Y, outdoor_temperature, long = get_data(data, step, loction)
# X=np.reshape(X,(X.shape[0],X.shape[-1]))
# X_train = X
# Y_train = Y
# 获取测试数据
# X_test, Y_test, point_temp = get_test_data_frome_TB(step, loction)
# X_test=np.reshape(X_test,(X_test.shape[0],X.shape[-1]))

step =1
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

# 获取测试数据
X_test, Y_test, point_temp = get_test_data_frome_TB(step, loction)


X_=[]
Y_=[]
arr=random_int_list(step, len(X), 100)
for i in  range(step,len(X)):
    if i not in arr:
        X_.append(X[i])
        Y_.append(Y[i])
X_train=np.concatenate(X_)
Y_train=np.concatenate(Y_)
X_train = np.reshape(np.array(X_train)[np.newaxis, :], (len(X_train) // step,len(X_train[0])))
Y_train= np.array(Y_train)



#随机选取几个测试
X_=[]
Y_=[]
for i in arr:
    X_.append(X[i])
    Y_.append(Y[i])
X_test=np.concatenate(X_)
Y_test=np.concatenate(Y_)
X_test = np.reshape(np.array(X_test)[np.newaxis, :], (len(X_test) // step,len(X_test[0])))
Y_tes=np.array(Y_test)


#制作测试标签
Y_te=[]
Y_tr=[]
for i in Y_test:
    Y_te.append([i])
for j in Y_train:
    Y_tr.append([j])
Y_train=np.array(Y_tr)
Y_test=np.array(Y_te)

print(X_train.shape)
print(Y_train.shape)

print(X_test.shape)
print(Y_test.shape)







#神经网络搭建
bp1 = BPNN.BPNNRegression([X_test.shape[-1], 16, 1])
train_data = [[sx.reshape(X.shape[-1],1), sy.reshape(1,1)] for sx, sy in zip(X_train, Y_train)]
test_data = [np.reshape(sx, (X.shape[-1],1)) for sx in X_test]
#神经网络训练
print(train_data)
start_time=time.process_time()
bp1.MSGD(train_data, 500, len(train_data), 0.2)
end_time=time.process_time()

#神经网络预测
y_predict=bp1.predict(test_data)
y_pre = np.array(y_predict)  # 列表转数组


y_pre=y_pre.reshape(len(y_predict),1)
y_pre=y_pre[:,0]
model_pred, Y_test = con_normalization(y_pre, Y_test)
print(y_pre)
#画图 #展示在测试集上的表现
plt.plot( y_pre, 'b-', label='model_pred')
plt.plot(Y_test, 'r-', label='Y_test')
plt.legend(loc='upper right',fontsize='15')
plt.title('BP {} flow+lux'.format(loction),fontsize='30') #添加标题
plt.show()
print(end_time-start_time)
#输出精度指标
# print('测试集上的MAE/MSE')
# print(mean_absolute_error(y_pre, Y_test))
# print(mean_squared_error(y_pre, Y_test) )
# mape = np.mean(np.abs((y_pre-Y_test)/(Y_test)))*100
# print('=============mape==============')
# print(mape,'%')
# # 画出真实数据和预测数据的对比曲线图
# print("R2 = ",metrics.r2_score(Y_test, y_pre)) # R2


