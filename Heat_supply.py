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



def data_processing(df,col_name):
    #将温度按0.5℃划分
    max_tp =math.ceil(df[col_name].max())
    min_tp=int(df[col_name].min())
    len_TP = (max_tp - min_tp + 1) * 2
    tab_temp = [round(float(min_tp + 0.5 * i), 2) for i in range(0, len_TP)]
    for j in range(len(df)):
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
    df_final.to_csv('C:/Users/nameless/Desktop/final.csv')
def get_data_scn():
    file =pd.read_csv('C:/Users/nameless/Desktop/final.csv')
    file = data_processing(file, 'C2_GS_T')
    print("1")
    file=data_processing(file, 'outdoor_temperature')
    print("2")
    file = data_processing(file, 'indoor_temperature_xmly_G')
    print(file)
    X_signals = []
    Y_signals = []
    for i in range(0,len(file)):
        X_signals.append([file['outdoor_temperature'].iloc[i],file['indoor_temperature_xmly_G'].iloc[i]])
        Y_signals.append([file['C2_GS_T'].iloc[i]])
    return X_signals,Y_signals

d,g=get_data_scn()
