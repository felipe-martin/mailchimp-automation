
'-----------------------------------------1. Imports------------------------------------------------'
# importando las librerias necesarias para el desarrollo del modelo

import pandas as pd
from utils.config import Config as cfg
from utils.functions import op_functions as fn
import sys
import warnings
warnings.filterwarnings("ignore")

date = fn.get_date()
date_format = '%Y-%m-%d %H:%M:%S'

email_configs = pd.read_excel('utils/email_config_database.xlsx')
email_configs = email_configs[email_configs['email_campaing_code']=='ENADAP']
email_template = email_configs['email_mailchimp_template'].iloc[0]
email_from_name = email_configs['email_from_name'].iloc[0]
email_reply_to = email_configs['email_reply_to'].iloc[0]
email_subject = email_configs['email_subject'].iloc[0]
email_send_flag = email_configs['email_automation_send_email_flag'].iloc[0]
email_campaign_name = f"Vitamina | Encuesta Adaptación | {date} | Powered by Maichimp-Automation"

# diccionario para crear audiencia
audience_creation_dictionary = {
    "audience_name" : f"Vitamina | Audiencia Encuesta Adaptación | {date} | Powered by Maichimp-Automation",
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

'-----------------------------------------2. Run------------------------------------------------'

if __name__ == "__main__":
    #Calling config files to connect tools 🧰
    cfg.get_credentials()
    mailchimp_client, sql_server_conn = cfg.connect_tools()

    #Automation process to send email
    contacts = fn.get_audience_list()
    audience_creation = fn.audience_creation_function(
                                            audience_creation_dictionary = audience_creation_dictionary, 
                                            mailchimp_client = mailchimp_client)
    audience_add_members_result = fn.add_members_to_audience_function(
                                            audience_id = audience_creation['id'],
                                            mail_list = contacts, 
                                            mailchimp_client = mailchimp_client)
    campaign_creation_result = fn.campaign_creation_function(
                                            campaign_name = email_campaign_name,
                                            audience_id = audience_creation['id'],
                                            subject = email_subject,
                                            from_name = email_from_name,
                                            reply_to = email_reply_to,
                                            template_id = email_template,
                                            mailchimp_client = mailchimp_client)
    #Verifica si esta activo el envio automatico
    if email_send_flag == 'true':
        fn.send_mail(campaign_id=campaign_creation_result['id'])
        print('[INFO] AUTOMATIC SEND DEACTIVATED...')
    



