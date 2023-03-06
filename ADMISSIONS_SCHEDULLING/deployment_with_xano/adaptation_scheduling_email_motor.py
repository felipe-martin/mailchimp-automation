# Usage: 
# python adaptation_scheduling_email_motor.py --campaing_email_code ENADAP 
'-----------------------------------------1. Imports------------------------------------------------'
# importando las librerias necesarias para el desarrollo del modelo

import pandas as pd
from utils.config import Config
from utils.functions import op_functions
import pyodbc
import sys
import argparse
import warnings
warnings.filterwarnings("ignore")
#Formato fecha
date_format = '%d-%m-%Y %H:%M:%S'

'-----------------------------------------2. Configuraciones------------------------------------------------'
#Argumentos solicitados via parametros
ap = argparse.ArgumentParser()

ap.add_argument("-c", "--campaing_email_code", required=True, type=str, 
    help='Codigo de campaña a utilizar')
args = ap.parse_args()

#Inicializar modulo de configuracion
cfg = Config()

#Conectar aplicaciones 🧰
MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD, ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4, ENDPOINT_5, ENDPOINT_6 = cfg.get_credentials()
mailchimp_client, sql_server_conn = cfg.connect_tools(MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD)

#Obtener fechas en distintos formatos
mailchimp_date, current_date, current_year = cfg.get_date()

#Obteniendo configuracion de la campaña especifica
#campaing_email_code = 'ENADAP'
campaing_email_code = args.campaing_email_code
email_template, email_from_name, email_reply_to, email_subject, email_send_flag, trigger_threshold_days = cfg.get_config_template(campaing_email_code)
email_campaign_name = f"Vitamina | Encuesta Adaptación | {str(mailchimp_date)} | Powered by Mailchimp-Automation"

# diccionario para crear audiencia
audience_creation_dictionary = {
    "audience_name" : f"Vitamina | Audiencia Encuesta Adaptación | {str(mailchimp_date)} | Powered by Mailchimp-Automation",
    "company" : "Vitamina | Salas Cuna y Jardines Infantiles",
    "address1" : "Oficina 1801",
    "address2" : "Avenida Apoquindo 4501",
    "state" : "Region Metropolitana",
    "city" :  "Las Condes",
    "zip_code" : "7550000",
    "country" : "CL", # EX: LK
    "from_name" : email_from_name,
    "from_email" : email_reply_to,
    "language" : "es"
} 

'-----------------------------------------3. Run------------------------------------------------'

if __name__ == "__main__":
    #Iniciar funciones para motor de envio
    fn = op_functions(mailchimp_client, sql_server_conn, current_year, ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4, ENDPOINT_5, ENDPOINT_6)
    
    #Proceso generacion de audiencia, creacion de campaña y posible envio
    threshold_days = trigger_threshold_days * -1
    contacts = fn.get_contacts(current_date, threshold_days, campaing_email_code)
    print(f"[INFO] //////////////////// THIS EMAIL CONFIGURATION HAS {email_send_flag} SENDING SET 📧... ////////////////////")

    # Continuar solo si hay mas de 1 registro en audiencia.
    if contacts.shape[0] > 3: 
        #Creacion de audiencia
        audience_creation = fn.audience_creation_function(
                                                audience_creation_dictionary = audience_creation_dictionary)
        #Agregar contactos a audiencia creada
        audience_add_members_result = fn.add_members_to_audience_function(
                                                audience_id = audience_creation['id'],
                                                mail_list = contacts)
        #Creacion de campaña
        campaign_creation_result = fn.campaign_creation_function(
                                                campaign_name = email_campaign_name,
                                                audience_id = audience_creation['id'],
                                                subject = email_subject,
                                                from_name = email_from_name,
                                                reply_to = email_reply_to,
                                                template_id = int(email_template))
        #Verifica si esta activo el envio automatico
        if email_send_flag == 'Automatico':
            fn.send_mail(campaign_id=campaign_creation_result['id'])
            print('[INFO] AUTOMATIC EMAIL SENDING DEACTIVATED...')
    else:
        print('[INFO] NOTHING TO SEND TODAY 🙈...')
        



