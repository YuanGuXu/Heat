import datetime
import math
import sys

from sqlalchemy import create_engine
import pandas as pd
from time import sleep
from tqdm import tqdm

conn1 = 'mysql+pymysql://zhangweijian:zhangweijian@192.168.10.11:3306/600ly_basic_data'
engine = create_engine(conn1, pool_pre_ping=True)

S1 = """ SELECT
CONCAT(SUBSTR(collection_time, 1, 13), ':00:00') AS time,



#燃气量
avg(RQB1_BK_Q) '燃气量RQB1',
avg(RQB2_BK_Q) '燃气量RQB2',

#一次侧情况-热表
avg(RB_GLF_SS_LL) '一次侧热表LL',
avg(RB_GLF_GS_T) '一次侧热表GST',
avg(RB_GLF_HS_T) '一次侧热表HST',

avg(RB_GLF_GS_T) '一网总管供水温度', 
avg(RB_GLF_HS_T) '一网总管回水温度',

#板换一次侧
#供水温度
avg(BH1_C1_GS_T) '一次侧低区供水温度',   #低区
avg(BH2_C1_GS_T) '一次侧中区供水温度',   #中区
avg(BH3_C1_GS_T) '一次侧高区供水温度',   #高区
avg(BH4_C1_GS_T)  '一次侧裙楼供水温度',   #裙楼
avg(BH5_ZG_C1_GS_T) '一次侧C高区区供水温度',  #C高区
#回水温度
avg(BH1_C1_HS_T)  '一次侧低区回水温度',   #低区
avg(BH2_C1_HS_T) '一次侧中区回水温度',   #中区
avg(BH3_C1_HS_T) '一次侧高区回水温度',   #高区
avg(BH4_C1_HS_T)'一次侧裙楼回水温度',   #裙楼
avg(BH5_ZG_C1_HS_T) '一次侧C高区回水温度',  #C高区

#板换二次侧
#供水温度
avg(BH1_C2_GS_T)'二次侧低区供水温度',   #低区
avg(BH2_C2_GS_T) '二次侧中区供水温度',   #中区
avg(BH3_C2_GS_T) '二次侧高区供水温度',   #高区
avg(BH4_C2_GS_T)  '二次侧裙楼供水温度',   #裙楼
avg(BH5_ZG_C2_GS_T) '二次侧C高区区供水温度',  #C高区
#回水温度
avg(BH1_C2_HS_T) '二次侧低区回水温度',   #低区
avg(BH2_C2_HS_T)'二次侧中区回水温度',   #中区
avg(BH3_C2_HS_T)'二次侧高区回水温度',   #高区
avg(BH4_C2_HS_T)'二次侧裙楼回水温度',   #裙楼
avg(BH5_ZG_C2_HS_T)'二次侧C高区回水温度',  #C高区
#流量
avg(BH1_SS_LL) '二次侧低区流量',   #低区
avg(BH2_SS_LL) '二次侧中区流量',   #中区
avg(BH3_SS_LL) '二次侧高区流量',   #高区
avg(BH4_SS_LL) '二次侧裙楼流量',   #裙楼
avg(BH5_SS_LL) '二次侧C高区流量',   #C高区

#立管热表
#供水温度
avg(RB_A_D_GS_T) '立管供水A-低区',  #A-低区
avg(RB_A_Z_GS_T) '立管供水A-中区',  #A-中区
avg(RB_A_G_GS_T) '立管供水A-高区',  #A-高区
avg(RB_B_D_GS_T) '立管供水B-低区',  #B-低区
avg(RB_B_Z_GS_T) '立管供水B-中区',  #B-中区
avg(RB_B_G_GS_T) '立管供水B-高区',  #B-高区
avg(RB_C_G_GS_T) '立管供水C',  #C
avg(RB_D_ZKL1_GS_T) '立管供水群-组空', #群-组空
avg(RB_D_KL1_GS_T) '立管供水群-南', #群-南
avg(RB_D_KL2_GS_T) '立管供水群-北', #群-北
#回水温度
avg(RB_A_D_HS_T) '立管回水A-低区',  #A-低区
avg(RB_A_Z_HS_T) '立管回水A-中区',  #A-中区
avg(RB_A_G_HS_T) '立管回水A-高区',  #A-高区
avg(RB_B_D_HS_T) '立管回水B-低区',  #B-低区
avg(RB_B_Z_HS_T)'立管回水B-中区',  #B-中区
avg(RB_B_G_HS_T) '立管回水B-高区',  #B-高区
avg(RB_C_G_HS_T) '立管回水C',  #C
avg(RB_D_ZKL1_HS_T) '立管回水群-组空', #群-组空
avg(RB_D_KL1_HS_T) '立管回水群-南', #群-南
avg(RB_D_KL2_HS_T) '立管回水群-北', #群-北
#流量
avg(RB_A_D_SS_LL) '立管流量A-低区',  #A-低区
avg(RB_A_Z_SS_LL) '立管流量A-中区',  #A-中区
avg(RB_A_G_SS_LL) '立管流量A-高区',  #A-高区
avg(RB_B_D_SS_LL) '立管流量B-低区',  #B-低区
avg(RB_B_Z_SS_LL) '立管流量B-中区',  #B-中区
avg(RB_B_G_SS_LL) '立管流量B-高区',  #B-高区
avg(RB_C_G_SS_LL) '立管流量c',  #C
avg(RB_D_ZKL1_SS_LL) '立管流量群-组空', #群-组空
avg(RB_D_KL1_SS_LL) '立管流量群-南', #群-南
avg(RB_D_KL2_SS_LL) '立管流量群-北' #群-北

FROM
 bd_xmly_mainstation_2021
where collection_time>"2021-11-02 00:00:00"
GROUP BY time 
 ;

 """
# where collection_time>"2021-01-06 00:00:00"

# ORDER BY
#  collection_time DESC
S2="""SELECT 
    CONCAT(SUBSTR(create_time, 1, 18), '0') AS collection_time,
    AVG(temp) outdoor_temperature,
    AVG(lux) outdoor_lux
FROM
    600ly_basic_data.bd_weather_station_2021
WHERE
    dev_id = 40002704
    and create_time>"2021-11-02 00:00:00"
GROUP BY create_time
;
"""

S3 = """SELECT 
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
        600ly_basic_data.bd_temp_hr_202104 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202105 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202106 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202107 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202108 UNION ALL SELECT 
        *
	FROM
        600ly_basic_data.bd_temp_hr_202109 UNION ALL SELECT 
		*
	FROM
        600ly_basic_data.bd_temp_hr_202110 UNION ALL SELECT 
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
        600ly_basic_data.bd_temp_hr_202203 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202204 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202205 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202206 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202207 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202208) sn_union
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
        600ly_basic_data.bd_temp_hr_202104 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202105 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202106 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202107 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202108 UNION ALL SELECT 
        *
	FROM
        600ly_basic_data.bd_temp_hr_202109 UNION ALL SELECT 
		*
	FROM
        600ly_basic_data.bd_temp_hr_202110 UNION ALL SELECT 
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
        600ly_basic_data.bd_temp_hr_202203 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202204 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202205 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202206 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202207 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202208) sn_union
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
        600ly_basic_data.bd_temp_hr_202104 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202105 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202106 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202107 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202108 UNION ALL SELECT 
        *
	FROM
        600ly_basic_data.bd_temp_hr_202109 UNION ALL SELECT 
		*
	FROM
        600ly_basic_data.bd_temp_hr_202110 UNION ALL SELECT 
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
        600ly_basic_data.bd_temp_hr_202203 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202204 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202205 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202206 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202207 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202208) sn_union
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
        600ly_basic_data.bd_temp_hr_202104 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202105 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202106 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202107 UNION ALL SELECT 
        *
    FROM
        600ly_basic_data.bd_temp_hr_202108 UNION ALL SELECT 
        *
	FROM
        600ly_basic_data.bd_temp_hr_202109 UNION ALL SELECT 
		*
	FROM
        600ly_basic_data.bd_temp_hr_202110 UNION ALL SELECT 
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
        600ly_basic_data.bd_temp_hr_202203 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202204 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202205 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202206 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202207 UNION ALL SELECT
				*
	FROM
        600ly_basic_data.bd_temp_hr_202208) sn_union
    WHERE
        dev_id IN (611110684 , 611090222, 611110680, 611110681, 20076698, 20045777)
    GROUP BY collection_time , dev_id) sn_xmly_CG ON sn_xmly_G.collection_time = sn_xmly_CG.collection_time
GROUP BY collection_time;
"""
print("start read sql......")
df1 = pd.read_sql_query(S1, engine)
print("Finsh read \n start save csv.........")

df1.to_csv('C:/Users/nameless/Desktop/S1.csv')

# df2 = pd.read_sql_query(S2, engine)
# df2.to_csv('C:/Users/nameless/Desktop/S2.csv')

# df3 = pd.read_sql_query(S3, engine)
