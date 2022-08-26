# -*- coding: utf-8 -*-
import datetime


import math
import sys
import numpy as np
from sqlalchemy import create_engine
import pandas as pd
import pymysql
from time import sleep
from tqdm import tqdm
conn1='mysql+pymysql://zhangweijian:zhangweijian@192.168.10.11:3306/600ly_basic_data'
engine = create_engine(conn1, pool_pre_ping=True)

S1=""" SELECT
collection_time,

#燃气量
RQB1_BK_Q,
RQB2_BK_Q,

#一次侧情况-热表
RB_GLF_SS_LL,
RB_GLF_GS_T,
RB_GLF_HS_T,

RB_GLF_GS_T '一网总管供水温度', 
RB_GLF_HS_T '一网总管回水温度',

#板换一次侧
#供水温度
BH1_C1_GS_T,   #低区
BH2_C1_GS_T,   #中区
BH3_C1_GS_T,   #高区
BH4_C1_GS_T,   #裙楼
BH5_ZG_C1_GS_T,  #C高区
#回水温度

BH1_C1_HS_T,   #低区
BH2_C1_HS_T,   #中区
BH3_C1_HS_T,   #高区
BH4_C1_HS_T,   #裙楼
BH5_ZG_C1_HS_T,  #C高区

#板换二次侧
#供水温度
BH1_C2_GS_T,   #低区
BH2_C2_GS_T,   #中区
BH3_C2_GS_T,   #高区
BH4_C2_GS_T,   #裙楼
BH5_ZG_C2_GS_T,  #C高区
#回水温度
BH1_C2_HS_T,   #低区
BH2_C2_HS_T,   #中区
BH3_C2_HS_T,   #高区
BH4_C2_HS_T,   #裙楼
BH5_ZG_C2_HS_T,  #C高区
#流量
BH1_SS_LL,   #低区
BH2_SS_LL,   #中区
BH3_SS_LL,   #高区
BH4_SS_LL,   #裙楼
BH5_SS_LL,   #C高区

#立管热表
#供水温度
RB_A_D_GS_T,  #A-低区
RB_A_Z_GS_T,  #A-中区
RB_A_G_GS_T,  #A-高区
RB_B_D_GS_T,  #B-低区
RB_B_Z_GS_T,  #B-中区
RB_B_G_GS_T,  #B-高区
RB_C_G_GS_T,  #C
RB_D_ZKL1_GS_T, #群-组空
RB_D_KL1_GS_T, #群-南
RB_D_KL2_GS_T, #群-北
#回水温度
RB_A_D_HS_T,  #A-低区
RB_A_Z_HS_T,  #A-中区
RB_A_G_HS_T,  #A-高区
RB_B_D_HS_T,  #B-低区
RB_B_Z_HS_T,  #B-中区
RB_B_G_HS_T,  #B-高区
RB_C_G_HS_T,  #C
RB_D_ZKL1_HS_T, #群-组空
RB_D_KL1_HS_T, #群-南
RB_D_KL2_HS_T, #群-北
#流量
RB_A_D_SS_LL,  #A-低区
RB_A_Z_SS_LL,  #A-中区
RB_A_G_SS_LL,  #A-高区
RB_B_D_SS_LL,  #B-低区
RB_B_Z_SS_LL,  #B-中区
RB_B_G_SS_LL,  #B-高区
RB_C_G_SS_LL,  #C
RB_D_ZKL1_SS_LL, #群-组空
RB_D_KL1_SS_LL, #群-南
RB_D_KL2_SS_LL #群-北


FROM
 `bd_xmly_mainstation_2020` ,
 `bd_xmly_mainstation_2021` 
 
# where collection_time>"2021-01-06 00:00:00"

# ORDER BY
#  collection_time DESC
 ;
 
 """

S2="""SELECT 
    CONCAT(SUBSTR(collection_time, 1, 18), '0') AS collection_time,
    AVG(temp) outdoor_temperature,
    AVG(lux) outdoor_lux
FROM
    600ly_basic_data.bd_weather_station_2020,
    600ly_basic_data.bd_weather_station_2021
WHERE
    dev_id = 40002704
GROUP BY collection_time;
"""

S3="""SELECT 
    sn_xmly_G.collection_time AS collection_time,
    AVG(indoor_temperature_xmly_G) indoor_temperature_xmly_G,
    AVG(indoor_temperature_xmly_Z) indoor_temperature_xmly_Z,
    AVG(indoor_temperature_xmly_D) indoor_temperature_xmly_D,
    AVG(indoor_temperature_xmly_CG) indoor_temperature_xmly_CG
FROM
    (SELECT 
        CONCAT(SUBSTR(collection_time, 1, 13), ':00:00') AS collection_time,
            dev_id,
            AVG(temp) indoor_temperature_xmly_G
    FROM
        (SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202111 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202112 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202201 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202202 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202203) sn_union
    WHERE
        dev_id IN (20045272 , 20045820, 20045803, 20045785, 20445788, 20445780, 20445792, 20445800)
    GROUP BY collection_time , dev_id) sn_xmly_G
        LEFT JOIN
    (SELECT 
        CONCAT(SUBSTR(collection_time, 1, 13), ':00:00') AS collection_time,
            dev_id,
            AVG(temp) indoor_temperature_xmly_Z
    FROM
        (SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202111 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202112 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202201 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202202 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202203) sn_union
    WHERE
        dev_id IN (611110694 , 20045801, 20045802, 20045776, 20045789, 20045795, 20045384, 20045796, 20445419, 20445821)
    GROUP BY collection_time , dev_id) sn_xmly_Z ON sn_xmly_G.collection_time = sn_xmly_Z.collection_time
        LEFT JOIN
    (SELECT 
        CONCAT(SUBSTR(collection_time, 1, 13), ':00:00') AS collection_time,
            dev_id,
            AVG(temp) indoor_temperature_xmly_D
    FROM
        (SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202111 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202112 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202201 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202202 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202203) sn_union
    WHERE
        dev_id IN (20045808 , 20045782, 20045784, 20045787, 20045822, 20045781, 20045387, 20045817)
    GROUP BY collection_time , dev_id) sn_xmly_D ON sn_xmly_G.collection_time = sn_xmly_D.collection_time
        LEFT JOIN
    (SELECT 
        CONCAT(SUBSTR(collection_time, 1, 13), ':00:00') AS collection_time,
            dev_id,
            AVG(temp) indoor_temperature_xmly_CG
    FROM
        (SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202111 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202112 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202201 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202202 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202203) sn_union
    WHERE
        dev_id IN (611110684 , 611090222, 611110680, 611110681, 20076698, 20045777)
    GROUP BY collection_time , dev_id) sn_xmly_CG ON sn_xmly_G.collection_time = sn_xmly_CG.collection_time
GROUP BY collection_time;
"""
import time
def progress_bar(df):
    import time
    print("\r", end="")
    print("Normalization progress: {}%: ".format(int(i / len(df) * 100)), "▋" * int(i / len(df) * 10), end="")
    sys.stdout.flush()
    time.sleep(0.05)

def data_processing(df,col_name):
    #将温度按0.5℃划分
    max_tp =math.ceil(df[col_name].max())
    min_tp=int(df[col_name].min())
    len_TP = (max_tp - min_tp + 1) * 2
    tab_temp = [round(float(min_tp + 0.5 * i), 2) for i in range(0, len_TP)]
    for j in range(len(df)):
        print(j,"/",len(df))
        for t in range(len(tab_temp)):
            if tab_temp[t] <= df[col_name].iloc[j] < tab_temp[t + 1]:
                df[col_name].iloc[j] = tab_temp[t]
    return df
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
    df_final.to_csv('C:/Users/nameless/Desktop/final_ALL.csv')
def get_data_scn(location):
    file =pd.read_csv('C:/Users/nameless/Desktop/final_ALL.csv',engine='python')
    print("Original data length:",len(file))
    file=file[(file['time']<'2022/3/30 23:00:00')]
    print("Before 2022/3/30:",len(file))
    file=file[file['S_side_Temp_{}'.format(location)]<50]
    print("二次侧供水温度小于 50：",len(file))
    # cols=['S_side_Temp_D','S_side_Temp_Z','S_side_Temp_G','S_side_Temp_QL','S_side_Temp_CG',
    #       'S_side_flow_D','S_side_flow_Z','S_side_flow_G','S_side_flow_QL','S_side_flow_CG',
    #       'outdoor_temperature','outdoor_lux','Inndoor_Temp_G','Inndoor_Temp_Z','Inndoor_Temp_D','Inndoor_Temp_CG']
    # for col in cols:
    #     print(col)
    #     file = data_processing(file,col)
    print("len_file")
    print(len(file))
    # file=normalization(file)
    X_signals = []
    Y_signals = []
    Out_door_temp=[]
    Out_door_lux=[]
    # num = np.arange(len(file))
    # np.random.shuffle(num)
    for i in range(0,len(file)):
        # X_signals.append([file['outdoor_temperature'].iloc[i],file['outdoor_lux'].iloc[i],file['Inndoor_Temp_{}'.format(location)].iloc[i],file['S_side_flow_{}'.format(location)].iloc[i]])
        X_signals.append([file['outdoor_temperature'].iloc[i],file['Inndoor_Temp_{}'.format(location)].iloc[i],file['outdoor_lux'].iloc[i]])
        Y_signals.append([file['S_side_Temp_{}'.format(location)].iloc[i]])
        Out_door_temp.append(file['outdoor_temperature'].iloc[i])#用于画图时作为对照
        Out_door_lux.append(file['outdoor_lux'].iloc[i])#用于画图时作为对照
    X_signals=np.array(X_signals)
    Y_signals=np.array(Y_signals)
    X_axi=file['time'].astype('datetime64')
    return X_signals,Y_signals,Out_door_temp,Out_door_lux,X_axi
def normalization(df):#归一化
    import time
    # cols=df.columns.values
    cols=['S_side_Temp_D','S_side_Temp_Z','S_side_Temp_G','S_side_Temp_CG','S_side_flow_D','S_side_flow_Z','S_side_flow_G','S_side_flow_CG','outdoor_temperature','outdoor_lux','Inndoor_Temp_G','Inndoor_Temp_Z','Inndoor_Temp_D','Inndoor_Temp_CG']
    for i in range(len(df)):
        # print('normalization:{}/{}'.format(i,len(df)))
        print("\r", end="")
        print("Normalization progress: {}%: ".format(int(i/len(df)*100)), "▋" * int(i/len(df)*10), end="")
        sys.stdout.flush()
        time.sleep(0.05)
        for col in cols[2:]:
            df[col].iloc[i]=(float(df[col].iloc[i])-float(df[col].min()))/(float(df[col].max())-float(df[col].min()))
    print("\n Normalization is complete")
    return df

def con_normalization(df):#反归一
    cols=df.columns.values
    for i in range(len(df)):
        print('con_normalization:{}/{}'.format(i,len(df)))
        for col in cols:
            df[col].iloc[i]=(df[col].iloc[i]*(df[col].max()-df[col].min())+df[col].min())
def get_data_from_tab(location):
    file = pd.read_csv('C:/Users/nameless/Desktop/heat_file_025.csv', engine='python')
    X_signals=[]
    Y_signals=[]
    x_axi=[]
    for i in range(0,len(file)):
        # X_signals.append([file['outdoor_temperature'].iloc[i],file['outdoor_lux'].iloc[i],file['Inndoor_Temp_{}'.format(location)].iloc[i],file['S_side_flow_{}'.format(location)].iloc[i]])
        X_signals.append([file['outdoor_temperature'].iloc[i],file['Inndoor_Temp_{}'.format(location)].iloc[i],file['outdoor_lux'].iloc[i]])
        Y_signals.append([file['S_side_Temp_{}'.format(location)].iloc[i]])
        x_axi.append([file['outdoor_temperature'].iloc[i]])
    X_signals=np.array(X_signals)
    Y_signals=np.array(Y_signals)
    return X_signals,Y_signals,x_axi
# file =pd.read_csv('C:/Users/nameless/Desktop/final_lux.csv')
# normalization(file)
# merg()