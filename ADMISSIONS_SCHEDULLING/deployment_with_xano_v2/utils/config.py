
'-----------------------------------------1. Imports------------------------------------------------'
import pyodbc
from mailchimp3 import MailChimp
import datetime
import boto3
import os, json
import pandas as pd
import warnings
import requests, json
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
            mailchimp_credentials = config["mailchimp_credentials"]
            sql_server_credentials = config["sql_server_credentials"]
            dynamodb_credentials = config["dynamodb_credentials"]
            xano_endpoints = config["xano_endpoints"]
       
        #Maichimp
        MAILCHIMP_USERNAME = mailchimp_credentials['user']
        MAILCHIMP_APIKEY = mailchimp_credentials['api-key']
        #SQL SERVER
        SQL_SERVER = sql_server_credentials['server']
        SQL_SERVER_DATABASE = sql_server_credentials['database']
        SQL_SERVER_USERNAME = sql_server_credentials['username']
        SQL_SERVER_PASSWORD = sql_server_credentials['password']
        #Xano
        ENDPOINT_1 = xano_endpoints["bulk_educational_center_admissions_endpoint"]
        ENDPOINT_2 = xano_endpoints["adaptation_schedulling_calendar_endpoint"]
        ENDPOINT_3 = xano_endpoints["current_educational_center_admissions_endpoint"]
        ENDPOINT_4 = xano_endpoints["monitoring_data_ingestion_endpoint"]
        ENDPOINT_5 = xano_endpoints["child_mail_journey_control_endpoint"]
        self.ENDPOINT_6 = xano_endpoints["mailchimp_config_template_endpoint"]

        return MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD, ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4, ENDPOINT_5, self.ENDPOINT_6


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

    
    def connect_tools(self, mailchimp_username, mailchimp_apikey, sql_server, sql_server_database, sql_server_username, sql_server_password):
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
        
        return self.client, self.cnxn
    

    def read_data_to_dataframe(self, endpoint_url):
        """
        Reads data from a specified endpoint URL as JSON and converts it to a Pandas DataFrame.
        
        Args:
        - endpoint_url (str): the URL of the endpoint to read the data from
        
        Returns:
        - pandas.DataFrame: the DataFrame containing the data read from the endpoint
        """
        print(f'[INFO] //////////// LECTURA TABLA DESDE XANO USANDO ENDPOINT... ////////////')
        try:
            # Make a GET request to the endpoint and get the response as JSON
            response = requests.get(endpoint_url)
            json_data = response.json()
            # Convert the JSON data to a DataFrame
            df = pd.DataFrame(json_data)
            print(f'[INFO] //////////// LECTURA TABLA CORRECTA... ////////////')
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO READ FROM XANO. PLEASE CHECK LOG... 🔍")    
            f = open("admissions_adaptation_schedulling.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        return df


    def get_config_template(self, campaing_email_code):
        mailchimp_templates = self.read_data_to_dataframe(self.ENDPOINT_6)
        selected_template = mailchimp_templates[mailchimp_templates['campaing_email_code']==campaing_email_code]
        print(f"[INFO] //////////////////// TEMPLATE SELECCIONADO: {campaing_email_code} 📧... ////////////////////")
        email_template = int(selected_template['mailchimp_html_template_code'].iloc[0])
        email_from_name = str(selected_template['email_from_name'].iloc[0])
        email_reply_to = str(selected_template['email_reply_to'].iloc[0])
        email_subject = str(selected_template['email_subject'].iloc[0])
        email_send_flag = str(selected_template['automation_send_email_flag'].iloc[0])
        trigger_threshold_days = int(selected_template['trigger_threshold_days'].iloc[0])


        return email_template, email_from_name, email_reply_to, email_subject, email_send_flag, trigger_threshold_days
