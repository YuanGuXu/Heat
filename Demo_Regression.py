# -*- coding: utf-8 -*-
import os
import pandas as pd
from SCN import SCN
import numpy
import numpy as np
import matplotlib.pyplot as plt
import scipy
np.seterr(divide='ignore', invalid='ignore')
from Heat_supply import *
from os.path import dirname
import smtplib


keys=['Z']
for key in keys:
    X,Y,Out_temp,Out_lux,X_axi=get_data_scn(key)
    X_test,Y_test,x_axi=get_data_from_tab(key)
    # Parameter Setting
    L_max = 400                 # maximum hidden node number
    tol = 0.001                    # training tolerance
    T_max = 100                    # maximun candidate nodes number
    Lambdas = [0.5, 1, 5, 10, 30, 50, 100, 150, 200, 250]  # scope sequence
    r = [0.9, 0.99, 0.999, 0.9999, 0.99999, 0.999999]  # 1-r contraction sequence
    nB = 1       # batch size
    verbose = 10
    # Model Initialization
    M = SCN(L_max, T_max, tol, Lambdas, r, nB, verbose)
    # load data
    T1 = Y
    X1 = X
    T2 = Y_test
    X2 =X_test
    Out_temp_1=Out_temp[:len(Out_lux)-10]
    Out_lux_1=Out_lux[:len(Out_lux)-10]
    X_axi_1=X_axi[:len(X)-10]

    Out_temp_2=Out_temp[len(Out_lux)-10:]
    Out_lux_2=Out_lux[len(Out_lux)-10:]
    X_axi_2=X_axi[len(X)-10:]
    # numpy.savetxt('C:/Users/nameless/Desktop/T1{}.csv'.format(key), T1, delimiter=',', header='T1')
    # numpy.savetxt('C:/Users/nameless/Desktop/T2{}.csv'.format(key), T2, delimiter=',', header='T2')
    per = M.regression(np.array(X1), np.array(T1))
    # plt.subplot(3, 1, 1)
    # plt.plot(range(0, per.shape[1]), per.reshape(-1, 1).tolist(), 'r.-')
    # # plt.axis(ymin=0, ymax=0.20)
    # plt.xticks(rotation=30)
    # plt.legend(['Training RMSE'])

    O1 = M.getOutput(X1)
    # numpy.savetxt('C:/Users/nameless/Desktop/O1{}.csv'.format(key), O1, delimiter=',',header ='O1')

    # plt.subplot(3, 1, 2)
    # plt.plot( X_axi_1,T1, 'r.-',linewidth=2.0)
    # plt.plot(X_axi_1, O1, 'b.-',linewidth=1.0)
    # plt.legend(['Train Target', 'Model Output', 'Out_temp'])
    # plt.xticks(rotation=30)
    # plt.twinx()
    # plt.plot(X_axi_1, Out_temp_1, 'k.-',linewidth=1.0)
    # plt.xticks(rotation=30)
    # plt.legend(['Out_temp'])


    O2 = M.getOutput(X2)
    print("**********")
    print(O2-T2)
    print(O2)
    print(T2)
    print("**********")
    # numpy.savetxt('C:/Users/nameless/Desktop/O2{}.csv'.format(key), O2, delimiter=',', header='O2')
    # for i in range(len(O2)) :
    #     print("\r", end="")
    #     print('mean:{}% '.format(i/len(O2)*100))
    #     if O2[i] >1:
    #         O2[i]=O1.mean()
    #     if O2[i]<0:
    #         O2[i] = O1.mean()
    # plt.subplot(3, 1, 3)
    plt.plot(x_axi,T2, 'r.-',linewidth=2.0)
    plt.plot(x_axi,O2, 'b.-',linewidth=1.0)
    # plt.xticks(rotation=30)
    plt.legend(['Test Target', 'Model Output'],loc='upper left', fontsize=9)
    # plt.legend(loc='upper left', fontsize=15)
    # plt.twinx()
    # plt.plot(X_axi_2, Out_temp_2, 'k.-',linewidth=1.0)
    # plt.xticks(rotation=30)
    # plt.legend('Out_.temp',loc='upper right', fontsize=9)
    # plt.legend( loc='upper right', fontsize=15)
    path = dirname(__file__) + '/picture'
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    name = '{}'.format(key)
    file_name = '{path}/{name}.png'.format(path=path, name=name)
    plt.savefig(file_name, dpi=300)
    plt.title(label='{}'.format(key))
    plt.show()
