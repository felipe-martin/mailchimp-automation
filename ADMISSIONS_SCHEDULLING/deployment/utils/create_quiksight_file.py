#Funciones
'--------------------------------1. Imports--------------------------------------------'

import pandas as pd
import sys
import boto3
import numpy as np
import os, json
import datetime
import warnings
import time
warnings.filterwarnings("ignore")

LMS_AWS_ACCESS_KEY_ID = "AKIA6DO53JJQCD5XCQKY"
LMS_AWS_SECRET_ACCESS_KEY = "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"
REGION = "us-east-1"
SERVICE_NAME="s3"
AA_SERVICE_LAYER_AWS_ACCESS_KEY_ID = "AKIAQRW6JXYJBL7IYU4P"
AA_SERVICE_LAYER_AWS_SECRET_ACCESS_KEY = "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"

date_format = "%d/%m/%Y %H:%M:%S"

#Configuracion e inicializacion del cliente dynamodb con boto3
def aws_dynamo_service(aws_access_key_id, aws_secret_access_key, region_name):
    try:
        #Realizando conexion con AWS DynamoDB
        conn = boto3.client("dynamodb", 
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
    except Exception as e:
        print("[INFO] No fue posible conectarse a la DB")    
        f = open("videofinder_log.txt", "a")
        f.write(f'{str(e)}\n')
        f.close()
    return conn

#Lectura de base de datos registros_evaluacion
def read_adaptation_schedulled_aws(table_name="practical_lms_adaptation_schedulled", client=None):
    print(f'[INFO] //////////// LECTURA TABLA {table_name} EN DYNAMODB ////////////')
    try:
        #Codigo para escanear tabla registros evaluacion desde dynamodb solo para registros no procesados por RPA
        response = client.scan(
            TableName=table_name)
        #Convertir el resultado de dynamoDB a DataFrame
        reg_ = pd.json_normalize(response["Items"])
        #Rename columns droping the dynamo json type (Ex. .S for String)
        for column in reg_.columns.to_list():
            reg_.rename(columns={column:column[:-2]}, inplace=True)
        #print("[INFO] Mostrando 5 primeros registros de evaluacion de la base cargada")
        #print(self.reg_.head())
        print('[INFO] //////////// LECTURA TABLA CORRECTA ////////////')
    except Exception as e:
        print("[INFO] No fue posible leer la base registros evaluacion desde DynamoDB")    
        f = open("videofinder_log.txt", "a")
        f.write(f'{str(e)}\n')
        f.close()
    return reg_


dynamodb = aws_dynamo_service(AA_SERVICE_LAYER_AWS_ACCESS_KEY_ID, AA_SERVICE_LAYER_AWS_SECRET_ACCESS_KEY, REGION)

s3 = boto3.resource(
    service_name=SERVICE_NAME,
    region_name=REGION,
    aws_access_key_id=LMS_AWS_ACCESS_KEY_ID,
    aws_secret_access_key=LMS_AWS_SECRET_ACCESS_KEY)
s3_client = boto3.client(
    service_name=SERVICE_NAME,
    region_name=REGION,
    aws_access_key_id=LMS_AWS_ACCESS_KEY_ID,
    aws_secret_access_key=LMS_AWS_SECRET_ACCESS_KEY) 

adaptation_schedulled = read_adaptation_schedulled_aws('educational_center_admissions', dynamodb)
adaptation_schedulled['child_service_id'] = adaptation_schedulled['child_service_id'].astype(int)
adaptation_schedulled = adaptation_schedulled.sort_values(by="child_service_id", ascending=False)

print(f'[INFO] //////////// REGISTROS LEIDOS: {adaptation_schedulled.shape[0]} ////////////')

#Tabla adaptation_schedulled
adaptation_schedulled['adaptations_total_Q'] = 1
adaptation_schedulled['adaptations_not_schedulled_flag'] = np.where(adaptation_schedulled['child_adaptation_scheduling_flag']=="wasnt scheduled",1,0)
adaptation_schedulled['adaptations_schedulled_flag'] = np.where(adaptation_schedulled['child_adaptation_scheduling_flag']=="true",1,0)

child_adaptation_scheduling_dt = []
for adaptation_dt_iter in range(adaptation_schedulled.shape[0]):
    try:
        adaptation_schedulled['child_adaptation_scheduling_dt'].iloc[adaptation_dt_iter] = pd.to_datetime(adaptation_schedulled['child_adaptation_scheduling_dt'].iloc[adaptation_dt_iter], format=date_format)
        adaptation_schedulled['child_adaptation_scheduling_date'] = [x.date() for x in adaptation_schedulled['child_adaptation_scheduling_dt']]
        adaptation_schedulled['child_adaptation_scheduling_day'] = [x.day for x in adaptation_schedulled['child_adaptation_scheduling_dt']]
        adaptation_schedulled['child_adaptation_scheduling_month'] = [x.month for x in adaptation_schedulled['child_adaptation_scheduling_dt']]
        adaptation_schedulled['child_adaptation_scheduling_year'] = [x.year for x in adaptation_schedulled['child_adaptation_scheduling_dt']]
    except:
        continue

#Tabla con email enviados
child_mail_journey = read_adaptation_schedulled_aws('child_mail_journey_control', dynamodb)
child_mail_journey['child_service_id'] = child_mail_journey['child_service_id'].astype(int)
print(f'[INFO] //////////// REGISTROS LEIDOS: {child_mail_journey.shape[0]} ////////////')

#Unir tablas para completar datos con mail enviado
adaptation_schedulled = adaptation_schedulled.merge(child_mail_journey, on='child_service_id', how='left')
adaptation_schedulled['child_adaptation_survey_email_sent_flag'] = adaptation_schedulled['child_adaptation_survey_email_sent_flag'].fillna('false') 
adaptation_schedulled['child_adaptation_survey_email_read_flag'] = adaptation_schedulled['child_adaptation_survey_email_read_flag'].fillna('false') 
adaptation_schedulled['child_adaptation_survey_completed_flag'] = adaptation_schedulled['child_adaptation_survey_completed_flag'].fillna('false') 
adaptation_schedulled['child_adaptation_survey_tag'] = adaptation_schedulled['child_adaptation_survey_tag'].fillna('false')
adaptation_schedulled['child_adaptation_survey_email_sent_dt'] = adaptation_schedulled['child_adaptation_survey_email_sent_dt'].fillna('')
adaptation_schedulled['child_welcome_email_sent_flag'] = adaptation_schedulled['child_welcome_email_sent_flag'].fillna('false')
adaptation_schedulled['child_welcome_email_read_flag'] = adaptation_schedulled['child_welcome_email_read_flag'].fillna('false') 
adaptation_schedulled['child_welcome_email_sent_dt'] = adaptation_schedulled['child_welcome_email_sent_dt'].fillna('') 
adaptation_schedulled['child_adaptation_scheduling_reminder_email_sent_flag'] = adaptation_schedulled['child_adaptation_scheduling_reminder_email_sent_flag'].fillna('false') 
adaptation_schedulled['child_adaptation_scheduling_reminder_email_read_flag'] = adaptation_schedulled['child_adaptation_scheduling_reminder_email_read_flag'].fillna('false')    
adaptation_schedulled['child_adaptation_scheduling_reminder_email_dt'] = adaptation_schedulled['child_adaptation_scheduling_reminder_email_dt'].fillna('false')

adaptation_schedulled.to_csv('adaptation_schedulling_monitoring.csv', index=False)

#Tabla con carga de datos
loaded_rows = read_adaptation_schedulled_aws('child_mail_journey_control', dynamodb)

#Subiendo csv a S3
response = s3_client.upload_file('adaptation_schedulling_monitoring.csv','lms-monitoring-bucket','adaptation_schedulling_monitoring.csv')
print('[INFO] //////////// TABLA ACTUALIZADA CORRECTAMENTE ////////////')