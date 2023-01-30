
'-----------------------------------------1. Imports------------------------------------------------'
import pyodbc
from mailchimp3 import MailChimp
import datetime
import os, json
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
       
        #Maichimp
        MAILCHIMP_USERNAME = mailchimp_credentials['user']
        MAILCHIMP_APIKEY = mailchimp_credentials['api-key']
        #SQL SERVER
        SQL_SERVER = sql_server_credentials['server']
        SQL_SERVER_DATABASE = sql_server_credentials['database']
        SQL_SERVER_USERNAME = sql_server_credentials['username']
        SQL_SERVER_PASSWORD = sql_server_credentials['password']

        return MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD


    def get_date(self):
        #Definiendo variable con fecha de proceso
        current_time = datetime.datetime.now()
        Y = current_time.year
        M = current_time.month
        d = current_time.day
        h = current_time.hour
        m = current_time.minute
        s = current_time.second
        mailchimp_format_date = f"{Y}{M}{d}"
        
        return mailchimp_format_date
    
    def connect_tools(self, mailchimp_username, mailchimp_apikey, sql_server, sql_server_database, sql_server_username, sql_server_password):
        # conexion con mailchimp API
        print("[INFO] //////////////////// CONNECTING TO MAILCHIMP 🐵... ////////////////////")
        try:
            self.client = MailChimp(mc_api = mailchimp_apikey, mc_user = mailchimp_username)
            print("[INFO] //////////////////// MAILCHIMP CONNECTED 🙊... ////////////////////")
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CONNECT MAILCHIMP. PLEASE CHECK CONFIG FILES 🙈")    
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
            print("[INFO] IT WASN'T POSSIBLE TO CONNECT SQL SERVER. PLEASE CHECK CONFIG FILES 🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return self.client, self.cnxn

