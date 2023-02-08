
'-----------------------------------------1. Imports------------------------------------------------'

import boto3
import pandas as pd
import numpy as np
import pyodbc
import warnings
import holidays
import datetime
import time
warnings.filterwarnings('ignore')


'-----------------------------------------2. Functions------------------------------------------------'

#Configuracion e inicializacion del cliente dynamodb con boto3
def aws_dynamo_service(aws_access_key_id, aws_secret_access_key, region_name):
    try:
        #Realizando conexion con AWS DynamoDB
        conn = boto3.resource("dynamodb", 
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        
        client = boto3.client("dynamodb", 
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key)
        
    except Exception as e:
        print("[INFO] No fue posible conectarse a la DB")    
        f = open("videofinder_log.txt", "a")
        f.write(f'{str(e)}\n')
        f.close()
    return conn, client


#Lectura de base de datos registros_evaluacion
def scan_dynamodb_table(table_name, client):
    print(f'[INFO] //////////// LECTURA TABLA {table_name} DESDE DYNAMODB ////////////')
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
        print(f'[INFO] //////////// LECTURA TABLA {table_name} CORRECTA ////////////')
    except Exception as e:
        print("[INFO] No fue posible leer la base registros evaluacion desde DynamoDB")    
        f = open("admissions_adaptation_schedulling.txt", "a")
        f.write(f'{str(e)}\n')
        f.close()
    return reg_


def update_item(table_name, client, key, update_expression, expression_attribute_values):

        print('[INFO] //////////// ACTUALIZANDO REGISTRO EN TABLA DYNAMODB ////////////')

        # Use the update_item method to update the item with the specified key
        try:
            response = client.update_item(
            TableName=table_name,
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW'
            )
            print('[INFO] //////////// UPDATE EJECUTADO CORRECTAMENTE ////////////')
        except Exception as e:
            print("[INFO] No fue posible leer la base registros evaluacion desde DynamoDB")    
            f = open("videofinder_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()

        return response


def get_date():
    #Definiendo variable con fecha de proceso
    current_time = datetime.datetime.now()
    date = current_time.date()
    Y = current_time.year
    M = current_time.month
    d = current_time.day
    h = current_time.hour
    m = current_time.minute
    s = current_time.second
    mailchimp_format_date = f"{Y}{M}{d}"
    
    return mailchimp_format_date, date, Y


'-----------------------------------------3. Credentials------------------------------------------------'

#dynamodb
AWS_ACCESS_KEY_ID = "AKIAQRW6JXYJBL7IYU4P"
AWS_SECRET_ACCESS_KEY = "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"
REGION = "us-east-1"
SERVICE_NAME="s3"

'-----------------------------------------4. Conexiones------------------------------------------------'

#dynamodb
dynamodb_resource, dynamodb_client = aws_dynamo_service(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION)
#Timestamp fields
mailchimp_date, current_date, current_year = get_date()
#Holidays in chile
cl_holidays = holidays.country_holidays('CL', years=[current_year, current_year+1])
holiday_dates = []
for date, occasion in cl_holidays.items():
    holiday_dates.append(date)

'-----------------------------------------6. Run------------------------------------------------'

if __name__ == "__main__":
    current_admisions = scan_dynamodb_table('educational_center_admissions', dynamodb_client)
    current_admisions['child_attendance_days_value'] = current_admisions['child_attendance_days_value'].astype(int)
    current_admisions['child_service_start_dt'] = pd.to_datetime(current_admisions['child_service_start_dt'])
    current_admisions['current_date'] = pd.to_datetime(current_date)
    current_admisions['working_days_difference'] = [np.busday_count(current_admisions['current_date'].iloc[x].date(), current_admisions['child_service_start_dt'].iloc[x].date(), holidays=holiday_dates) for x in range(current_admisions.shape[0])] 

    for i, admission in current_admisions.iterrows():
        if (admission['child_adaptation_scheduling_flag']=='false') & ((admission['working_days_difference'] <= -10) | (admission['child_attendance_days_value'] >= 6)):

            #Update child_adaptation_scheduling_dt
            update_expression = "SET child_adaptation_scheduling_dt = :x"
            expression_attribute_values = {':x': {'S': '31/12/2050 09:00:00'}}
            key = {'child_service_id': {'N': admission['child_service_id']}}
            updated_item_1 = update_item('educational_center_admissions', dynamodb_client, key, update_expression, expression_attribute_values)
            updated_item_2 = update_item('adaptation_schedulling_calendar', dynamodb_client, key, update_expression, expression_attribute_values)

            #Update child_adaptation_responsible
            update_expression = "SET child_adaptation_responsible = :x"
            expression_attribute_values = {':x': {'S': 'No agendado'}}
            key = {'child_service_id': {'N': admission['child_service_id']}}
            updated_item_1 = update_item('educational_center_admissions', dynamodb_client, key, update_expression, expression_attribute_values)
            updated_item_2 = update_item('adaptation_schedulling_calendar', dynamodb_client, key, update_expression, expression_attribute_values)

            #Update child_adaptation_scheduling_comment
            update_expression = "SET child_adaptation_scheduling_comment = :x"
            expression_attribute_values = {':x': {'S': 'Adaptación no agendada por aplicación'}}
            key = {'child_service_id': {'N': admission['child_service_id']}}
            updated_item_1 = update_item('educational_center_admissions', dynamodb_client, key, update_expression, expression_attribute_values)
            updated_item_2 = update_item('adaptation_schedulling_calendar', dynamodb_client, key, update_expression, expression_attribute_values)

            #Update first_interview_flag
            update_expression = "SET first_interview_flag = :x"
            expression_attribute_values = {':x': {'S': 'Sin respuesta'}}
            key = {'child_service_id': {'N': admission['child_service_id']}}
            updated_item_1 = update_item('educational_center_admissions', dynamodb_client, key, update_expression, expression_attribute_values)
            updated_item_2 = update_item('adaptation_schedulling_calendar', dynamodb_client, key, update_expression, expression_attribute_values)

            #Update child_adaptation_scheduling_flag
            update_expression = "SET child_adaptation_scheduling_flag = :x"
            expression_attribute_values = {':x': {'S': 'wasnt scheduled'}}
            key = {'child_service_id': {'N': admission['child_service_id']}}
            updated_item_1 = update_item('educational_center_admissions', dynamodb_client, key, update_expression, expression_attribute_values)