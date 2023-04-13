
'-----------------------------------------1. Imports------------------------------------------------'
import pyodbc
from mailchimp3 import MailChimp
import datetime
import boto3
import os, json
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


class Config:

    def __init__(self):
        print("[INFO] //////////////////// CREDENTIALS MODULE STARTED 🤘🏻... ////////////////////")

    def get_credentials(self, path='/config.json'):
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        config_path = '/'.join([ROOT_DIR, path])
        #Configuraciones
        with open(config_path) as config_file:
            config = json.load(config_file)
            mailchimp_credentials = config['mailchimp_credentials']
            sql_server_credentials = config['sql_server_credentials']
            dynamodb_credentials = config['dynamodb_credentials']
       
        #Maichimp
        MAILCHIMP_USERNAME = mailchimp_credentials['user']
        MAILCHIMP_APIKEY = mailchimp_credentials['api-key']
        #SQL SERVER
        SQL_SERVER = sql_server_credentials['server']
        SQL_SERVER_DATABASE = sql_server_credentials['database']
        SQL_SERVER_USERNAME = sql_server_credentials['username']
        SQL_SERVER_PASSWORD = sql_server_credentials['password']
        #DynamoDB
        AWS_ACCESS_KEY_ID = dynamodb_credentials['aws_access_key_id']
        AWS_SECRET_ACCESS_KEY = dynamodb_credentials['aws_secret_access_key']
        AWS_REGION = dynamodb_credentials['region']

        return MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION


    def get_date(self):
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

    
    def connect_tools(self, mailchimp_username, mailchimp_apikey, sql_server, sql_server_database, sql_server_username, sql_server_password, aws_access_key_id, aws_secret_access_key, region_name):
        # conexion con mailchimp API
        print("[INFO] //////////////////// CONNECTING TO MAILCHIMP 🐵... ////////////////////")
        try:
            self.client = MailChimp(mc_api = mailchimp_apikey, mc_user = mailchimp_username)
            print("[INFO] //////////////////// MAILCHIMP CONNECTED 🙊... ////////////////////")
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CONNECT MAILCHIMP. PLEASE CHECK LOG 🙈")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        # conexion con sql server
        print("[INFO] //////////////////// CONNECTING TO SQL SERVER 💾... ////////////////////")
        try:
            # conexion con sql server
            server = sql_server
            database = sql_server_database
            username = sql_server_username
            password = sql_server_password
            self.cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)

            print("[INFO] //////////////////// SQL SERVER CONNECTED 🧑🏻‍💻... ////////////////////")
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CONNECT SQL SERVER. PLEASE CHECK LOG 🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
         # conexion con dynamodb
        print("[INFO] //////////////////// CONNECTING TO DYNAMODB 🤖... ////////////////////")
        try:
            #Realizando conexion con AWS DynamoDB
            self.dynamo_resource = boto3.resource("dynamodb", 
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key)
            
            self.dynamo_client = boto3.client("dynamodb", 
                region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key)
            
            print("[INFO] //////////////////// DYNAMODB CONNECTED 🧑🏻‍💻... ////////////////////")
        
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CONNECT DYNAMODB. PLEASE CHECK LOG🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return self.client, self.cnxn, self.dynamo_resource, self.dynamo_client


    #Lectura de base de datos registros_evaluacion
    def get_template(self, table_name, client):
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
            print("[INFO] IT WASN'T POSSIBLE TO READ FROM DYNAMODB. PLEASE CHECK LOG 🔍")    
            f = open("admissions_adaptation_schedulling.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        return reg_


    def get_config_template(self, campaing_email_code, dynamodb_client):
        mailchimp_templates = self.get_template('mailchimp_config_template', dynamodb_client)
        selected_template = mailchimp_templates[mailchimp_templates['campaing_email_code']==campaing_email_code]
        print(f"[INFO] //////////////////// TEMPLATE SELECCIONADO: {campaing_email_code} 📧... ////////////////////")
        email_template = int(selected_template['mailchimp_html_template_code'].iloc[0])
        email_from_name = str(selected_template['email_from_name'].iloc[0])
        email_reply_to = str(selected_template['email_reply_to'].iloc[0])
        email_subject = str(selected_template['email_subject'].iloc[0])
        email_send_flag = str(selected_template['automation_send_email_flag'].iloc[0])
        trigger_threshold_days = int(selected_template['trigger_threshold_days'].iloc[0])


        return email_template, email_from_name, email_reply_to, email_subject, email_send_flag, trigger_threshold_days
