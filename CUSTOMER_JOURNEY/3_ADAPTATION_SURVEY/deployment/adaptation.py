# Usage:
# python welcome.py --campaing_email_code WELCOME 

"-----------------------------------------1. Imports------------------------------------------------"

import sys, json
import warnings
import pandas as pd
import pyodbc
import argparse
from utils.config import Config
from utils.functions import op_functions

warnings.filterwarnings("ignore")

"-----------------------------------------2. Functions------------------------------------------------"
# Argumentos solicitados via parametros
ap = argparse.ArgumentParser()
ap.add_argument("-c", "--campaing_email_code", required=True, type=str, 
    help='Codigo de campaña a utilizar')
args = ap.parse_args()

"-----------------------------------------3. Credentials and connections------------------------------------------------"
#Inicializar modulo de configuracion
cfg = Config()

#Conectar aplicaciones 🧰
MAILCHIMP_USERNAME, \
MAILCHIMP_APIKEY, \
SQL_SERVER, \
SQL_SERVER_DATABASE, \
SQL_SERVER_USERNAME, \
SQL_SERVER_PASSWORD, \
ENDPOINT_1, \
ENDPOINT_2, \
ENDPOINT_3, \
ENDPOINT_4, \
ENDPOINT_5,\
ENDPOINT_6, \
ENDPOINT_7 = cfg.get_credentials()
mailchimp_client, sql_server_conn = cfg.connect_tools(MAILCHIMP_USERNAME, 
                                                      MAILCHIMP_APIKEY, 
                                                      SQL_SERVER, 
                                                      SQL_SERVER_DATABASE, 
                                                      SQL_SERVER_USERNAME, 
                                                      SQL_SERVER_PASSWORD)
#sql server
#server = "10.10.5.7,1433" 
#database = "vitamina" 
#username = "consulta" 
#password = "consulta1" 

# Obtener fechas en distintos formatos
mailchimp_date, current_date, current_year = cfg.get_date()

# Obteniendo configuracion de la campaña especifica
campaing_email_code = args.campaing_email_code
email_template, email_from_name, email_reply_to, email_subject, email_send_flag, trigger_threshold_days = cfg.get_config_template(campaing_email_code)
threshold_days = trigger_threshold_days * -1

print("Cantidad de dias a observar: ", threshold_days)
print("Asunto de la campaña: ", email_subject)

# Iniciacion de modulo funciones
fn = op_functions(mailchimp_client, 
                  sql_server_conn, 
                  current_year, 
                  ENDPOINT_1, 
                  ENDPOINT_2, 
                  ENDPOINT_3, 
                  ENDPOINT_4,
                  ENDPOINT_5, 
                  ENDPOINT_6,
                  ENDPOINT_7)

"-----------------------------------------6. Run------------------------------------------------"

if __name__ == "__main__":

    # Obteniendo nuevos ingresos para enviar mail de adaptacion
    contacts = fn.get_end_adaptations(current_date, threshold_days, campaing_email_code)
    print("Contactos a cargar: \n", contacts.shape[0])
    # Cargando nuevos ingresos en audiencia para activar journey en Mailchimp
    fn.add_members_to_adaptation_journey('de22bb4365',contacts)
    print("[INFO] //////////// PROCESO EJECUTADO CORRECTAMENTE 🤘🏻 ////////////")
