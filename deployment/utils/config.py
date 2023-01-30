
'-----------------------------------------1. Imports------------------------------------------------'
import pyodbc
from mailchimp3 import MailChimp
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
        self.MAILCHIMP_USERNAME = mailchimp_credentials['user']
        self.MAILCHIMP_APIKEY = mailchimp_credentials['api-key']
        #SQL SERVER
        self.SQL_SERVER = sql_server_credentials['server']
        self.SQL_SERVER_DATABASE = sql_server_credentials['database']
        self.SQL_SERVER_USERNAME = sql_server_credentials['username']
        self.SQL_SERVER_PASSWORD = sql_server_credentials['password']
    
    def connect_tools(self):
        # conexion con mailchimp API
        print("[INFO] //////////////////// CONNECTING TO MAILCHIMP 🐵... ////////////////////")
        try:
            self.client = MailChimp(mc_api=self.MAILCHIMP_APIKEY, mc_user=self.MAILCHIMP_USERNAME)
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
            server = self.SQL_SERVER
            database = self.SQL_SERVER_DATABASE
            username = self.SQL_SERVER_USERNAME
            password = self.SQL_SERVER_PASSWORD
            cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
            self.cursor = cnxn.cursor()
            print("[INFO] //////////////////// MAILCHIMP CONNECTED 🧑🏻‍💻... ////////////////////")
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CONNECT SQL SERVER. PLEASE CHECK CONFIG FILES 🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return self.client, self.cursor

