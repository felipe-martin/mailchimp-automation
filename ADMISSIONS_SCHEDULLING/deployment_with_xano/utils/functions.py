'-----------------------------------------1. Imports------------------------------------------------'

import pandas as pd
import requests, json
import pyodbc
import numpy as np
import holidays
from tqdm import tqdm
import time
import warnings
warnings.filterwarnings("ignore")
date_format = '%d/%m/%Y %H:%M:%S'
enrollment_date_format = '%Y/%m/%d %H:%M:%S'


'-----------------------------------------2. Functions------------------------------------------------'

class op_functions:

    def __init__(self, mailchimp_client, sql_server_conn, current_year, endpoint_url_1, endpoint_url_2, endpoint_url_3, endpoint_url_4, endpoint_url_5, endpoint_url_6):
        print("[INFO] //////////////////// MAILCHIMP AUTOMATION MODULE ACTIVE 📧... ////////////////////")
        self.SQL_SERVER_CONN = sql_server_conn
        self.MAILCHIMP_CLIENT = mailchimp_client
        self.ENDPOINT_1 = endpoint_url_1
        self.ENDPOINT_2 = endpoint_url_2
        self.ENDPOINT_3 = endpoint_url_3
        self.ENDPOINT_4 = endpoint_url_4
        self.ENDPOINT_5 = endpoint_url_5
        self.ENDPOINT_6 = endpoint_url_6
        cl_holidays = holidays.country_holidays('CL', years=[current_year, current_year+1])
        self.holiday_dates = []
        for date, occasion in cl_holidays.items():
            self.holiday_dates.append(date)

    
    #Lectura de base de datos registros_evaluacion
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
    

    def post_mail_journey_control(self, endpoint_url, dataframe):
        """
        Posts each row of a Pandas DataFrame as a JSON object to a specified endpoint URL.
        
        Args:
        - endpoint_url (str): the URL of the endpoint to which to post the data
        - df (pandas.DataFrame): the DataFrame containing the rows to post
        
        Returns:
        - None
        """
        # Convert each row of the DataFrame to a JSON object and post it to the endpoint
        for j, row in tqdm(dataframe.iterrows(), "Loading Xano DB from Contacts:"):
            item = {"item": {
                'child_service_id': int(row['child_service_id']),
                'child_adaptation_survey_email_sent_flag': "true",
                'child_adaptation_survey_email_read_flag': "false",
                'child_adaptation_survey_completed_flag': "false",
                'child_adaptation_survey_tag': str(row['TAG']),
                'child_adaptation_survey_email_sent_dt': str(row['current_date']),
                'child_welcome_email_sent_flag': "false",
                'child_welcome_email_read_flag': "false",
                'child_welcome_email_sent_dt': "",
                'child_adaptation_scheduling_reminder_email_sent_flag': "false",
                'child_adaptation_scheduling_reminder_email_read_flag': "false",
                'child_adaptation_scheduling_reminder_email_dt': "false"
            }}
            try:
                response = requests.post(endpoint_url, json=item)
                #print(f"[INFO] //////////// BATCH EJECUTADO CORRECTAMENTE ////////////")
            except Exception as e:
                print("//////////// ERROR EN EJECUCION DE BATCH. REVISAR LOG ////////////")    
                f = open("admissions_adaptation_schedulling.txt", "a")
                f.write(f"{str(e)}\n")
                f.close()
        print(f"[INFO] //////////s// BATCH EJECUTADO CORRECTAMENTE ////////////")

    
    def post_mail_journey_monitor(self, endpoint_url, dataframe, process_step, process_name):
        """
        Posts each row of a Pandas DataFrame as a JSON object to a specified endpoint URL.
        
        Args:
        - endpoint_url (str): the URL of the endpoint to which to post the data
        - df (pandas.DataFrame): the DataFrame containing the rows to post
        
        Returns:
        - None
        """
        # Convert each row of the DataFrame to a JSON object and post it to the endpoint
        for j, row in tqdm(dataframe.iterrows(), "Loading Xano DB mail journey monitor from dataframe:"):
            item = {"item": {
                'child_service_id': int(row['child_service_id']),
                'process_email_step_value': str(process_step),  
                'process_email_dt': str(row['current_date']),
                'process_email_name': str(process_name),
                'process_email_key': str(row['child_service_id']) + "-" + str(process_name),
                'process_email_tag': str(row['TAG'])
            }}
            try:
                response = requests.post(endpoint_url, json=item)
                #print(f"[INFO] //////////// BATCH EJECUTADO CORRECTAMENTE ////////////")
            except Exception as e:
                print("//////////// ERROR EN EJECUCION DE BATCH. REVISAR LOG ////////////")    
                f = open("admissions_adaptation_schedulling.txt", "a")
                f.write(f"{str(e)}\n")
                f.close()
        print(f"[INFO] //////////s// BATCH EJECUTADO CORRECTAMENTE ////////////")
    

    def get_new_admissions(self, admissions, current_date, days):
    
        print("[INFO] //////////// GETTING LIST NEW ADMISSIONS TO SEND WELCOME EMAIL 🙈 ////////////")

        columns = [
        'child_service_id',
        'child_vitamina_id',
        'child_name',
        'child_last_enrollment_dt',
        'child_educational_guardian_id',
        'child_financial_guardian_email',
        'child_educational_guardian_email'
        ]

        # Lectura de base de datos adaptacion desde Xanodb.
        #educational_center_admissions = self.read_data_to_dataframe(self.ENDPOINT_3)
        educational_center_admissions = admissions 
        # Filtrar columnas
        educational_center_admissions = educational_center_admissions[columns]
        # Creacion de columnas de interes
        educational_center_admissions['child_last_enrollment_dt'] = pd.to_datetime(educational_center_admissions['child_last_enrollment_dt'], format=enrollment_date_format) 
        educational_center_admissions['date'] = [x.date() for x in educational_center_admissions.child_last_enrollment_dt]
        educational_center_admissions['current_date'] = current_date
        educational_center_admissions['days_difference'] = educational_center_admissions['date'] - educational_center_admissions['current_date']
        educational_center_admissions['days_difference'] = [x.days for x in educational_center_admissions['days_difference']]
        # Filtro seleccion de audiencia
        educational_center_admissions = educational_center_admissions[educational_center_admissions['days_difference']==days]
        
        # Seleccionar campos necesarios para cargar audiencia
        columns = [
            'child_educational_guardian_email',
            'child_financial_guardian_email',
        ]
        educational_center_admissions = educational_center_admissions[columns]
        audience = []

        # Creacion de array para almacenar los correos a agregar
        if educational_center_admissions.shape[0]!=0:
            
            email_list_1 = educational_center_admissions['child_educational_guardian_email'].to_list() # Lista de correos apoderados
            email_list_2 = educational_center_admissions['child_financial_guardian_email'].to_list() # Lista de correos apoderado financiero
            for email in email_list_1:
                audience.append(email) # Cargar listado de correos en audiencia
            for email in email_list_2:
                audience.append(email) # Cargar listado de correos en audiencia
            
            audience = pd.DataFrame(audience, columns=["Email"]) # Crear dataframe cambiando nombre a column Email
            audience = audience.drop_duplicates(subset="Email") # Eliminar correos duplicados

        else:
            print("[INFO] //////////// EMPTY LIST. NOTHING TO SEND TODAY 🙈 ////////////")

        return audience
            
    
    def get_contacts(self, current_date, trigger_threshold_days, campaing_email_code):
        columns = [
        'child_service_id',
        'child_vitamina_id',
        'child_name',
        'child_educational_guardian_id',
        'child_financial_guardian_email',
        'child_educational_guardian_email',
        'child_adaptation_scheduling_dt',
        'child_adaptation_scheduling_flag'
        ]

        TRIGGER_THRESHOLD_DAYS = trigger_threshold_days
        
        # Lectura de base de datos adaptacion desde dynamodb.
        educational_center_admissions = self.read_data_to_dataframe(self.ENDPOINT_3)
        # Filtrar columnas
        educational_center_admissions = educational_center_admissions[columns]
        # Filtro para quedarnos solo con adaptaciones agendadas.
        educational_center_admissions = educational_center_admissions[educational_center_admissions['child_adaptation_scheduling_flag']=="true"]
        # Creacion de columnas de interes
        educational_center_admissions['child_adaptation_scheduling_dt'] = pd.to_datetime(educational_center_admissions['child_adaptation_scheduling_dt'], format=date_format) 
        educational_center_admissions['date'] = [x.date() for x in educational_center_admissions.child_adaptation_scheduling_dt]
        educational_center_admissions['current_date'] = current_date
        educational_center_admissions['days_difference'] = educational_center_admissions['date'] - educational_center_admissions['current_date']
        educational_center_admissions['days_difference'] = [x.days for x in educational_center_admissions['days_difference']]
        educational_center_admissions['working_days_difference'] = [np.busday_count(educational_center_admissions['current_date'].iloc[x], educational_center_admissions['date'].iloc[x], holidays=self.holiday_dates) for x in range(educational_center_admissions.shape[0])] 
        educational_center_admissions['child_service_id'] = educational_center_admissions['child_service_id'].astype(int)
        educational_center_admissions['TAG'] = [(str(educational_center_admissions['child_service_id'].iloc[x]) + str(educational_center_admissions['child_vitamina_id'].iloc[x]) + str(educational_center_admissions['child_educational_guardian_id'].iloc[x]) + str(educational_center_admissions['current_date'].iloc[x])).replace("-","") for x in range(educational_center_admissions.shape[0])]
        educational_center_admissions['TIPO'] = campaing_email_code
        educational_center_admissions['send_email_flag'] = np.where(educational_center_admissions['working_days_difference']==TRIGGER_THRESHOLD_DAYS, 'Enviar', 'No enviar')
        
        # Creacion de audiencia utilizando tag
        #audience = educational_center_admissions.merge(tags, how='left', on='child_service_id')
        print("[INFO] /////////////////// SHOWING GENERATED DATA... ///////////////////")
        print(educational_center_admissions.head())

        columns = [
            'child_educational_guardian_email',
            'TAG',
            'TIPO'
        ]
        audience = educational_center_admissions[educational_center_admissions['send_email_flag']=='Enviar']
        #Eliminar registros sin correo electronico para seguridad
        audience['child_educational_guardian_email'] = np.where(audience['child_educational_guardian_email']=="", np.nan, audience['child_educational_guardian_email'])
        audience.dropna(subset=['child_educational_guardian_email'], inplace=True)
        if audience.shape[0] >= 1:
            #Marcar a los que enviaremos correos
            print("[INFO] /////////////////// INSERTING DATA TO JOURNEY CONTROL TABLE... ///////////////////")
            self.post_mail_journey_control(self.ENDPOINT_5, audience)
            
        #Seleccionar campos necesarios para cargar audiencia
        audience = audience[columns]
        audience = audience.rename(columns={'child_educational_guardian_email': 'Email' })
        
        #Agregar mail de prueba.
        indicator_light_email = {
            'Email': 'jaime.arroyo@vitamina.com', 
            'TAG': '18356794220230206', 
            'TIPO': campaing_email_code}
        audience = audience.append(indicator_light_email, ignore_index=True)
        #Segundo mail de prueba
        indicator_light_email = {
            'Email': 'javiera.carter@vitamina.com', 
            'TAG': '1812312920230206', 
            'TIPO': campaing_email_code}
        audience = audience.append(indicator_light_email, ignore_index=True)
        #Tercer mail de prueba
        indicator_light_email = {
            'Email': 'camila.saa@vitamina.com', 
            'TAG': '1915289320230206', 
            'TIPO': campaing_email_code}
        audience = audience.append(indicator_light_email, ignore_index=True)

        print("[INFO] /////////////////// SHOWING AUDIENCE TO SEND EMAIL ... ///////////////////")
        print(audience.head())

        return audience

    
    #Funcion para crear audiencia segun campos solicitados
    def audience_creation_function(self, audience_creation_dictionary):

        print("[INFO] //////////////////// CREATING AUDIENCE IN MAILCHIMP 🐵... ////////////////////")
            
        #prepare de variable to receive the audience creation
        audience_creation = ''

        #defining a variable with the dictionary that we need to create
        audience_creation_dictionary = audience_creation_dictionary
        print(audience_creation_dictionary)

        audience_list = {
            "name": audience_creation_dictionary['audience_name'],
            "contact":
            {
                "company": audience_creation_dictionary['company'],
                "address1": audience_creation_dictionary['address1'],
                "address2": audience_creation_dictionary['address2'],
                "city": audience_creation_dictionary['city'],
                "state": audience_creation_dictionary['state'],
                "zip": audience_creation_dictionary['zip_code'],
                "country": audience_creation_dictionary['country']
            },
            "permission_reminder": '.',
            "campaign_defaults":
            {
                "from_name": audience_creation_dictionary['from_name'],
                "from_email": audience_creation_dictionary['from_email'],
                "subject": "",
                "language": audience_creation_dictionary['language']
            },
            "email_type_option": False
        }

        try:
            audience_creation = self.MAILCHIMP_CLIENT.lists.create(data = audience_list)
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CREATE AUDIENCE. PLEASE CHECK LOG  🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return audience_creation
    

    #Funcion para agregar miembos a la audiencia
    def add_members_to_welcome_journey(self, audience_id, mail_list):
        
        print("[INFO] //////////////////// ADDING MEMBERS TO AUDIENCE IN MAILCHIMP 🙋🏻‍♀️ > 🐵... ////////////////////")
        audience_id = audience_id
        # debe ser un dataframe
        email_list = mail_list

        if len(email_list)!=0:
            for i_, email_iteration in email_list.iterrows():
                try:
                    data = {
                        "email_address" : email_iteration['Email'],
                        "status": "subscribed"                        
                    }
                    self.MAILCHIMP_CLIENT.lists.members.create(list_id=audience_id, data=data)
                    print('[INFO] {} HAS BEEN SUCCESSFULLY ADDED TO THE {} AUDIENCE'.format(email_iteration, audience_id))

                except Exception as e:
                    print("[INFO] IT WASN'T POSSIBLE TO ADD MEMBERS TO AUDIENCE. PLEASE CHECK LOG  🔍")    
                    f = open("automatizacion_mailchimp_log.txt", "a")
                    f.write(f'{str(e)}\n')
                    f.close()
        else: 
            print("[INFO] EMPTY LIST. PLEASE CHECK QUERY") 
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write('Nothing to send. Empty list. \n')
            f.close()


    #Funcion para agregar miembos a la audiencia
    def add_members_to_audience_function(self, audience_id, mail_list):
        
        print("[INFO] //////////////////// ADDING MEMBERS TO AUDIENCE IN MAILCHIMP 🙋🏻‍♀️ > 🐵... ////////////////////")
        audience_id = audience_id
        # debe ser un dataframe
        email_list = mail_list

        #merging values before to add contacts

        # merge campos de la data
        self.MAILCHIMP_CLIENT.lists.merge_fields.create(list_id=audience_id, data={
            'tag': 'TAG',
            'name': 'TAG',
            'type': 'text',
            'required': False,
            'default_value': '',
            'public': True
        })


        self.MAILCHIMP_CLIENT.lists.merge_fields.create(list_id=audience_id, data={
            'tag': 'TIPO',
            'name': 'TIPO',
            'type': 'text',
            'required': False,
            'default_value': '',
            'public': True
        })

        if len(email_list)!=0:
            for i_, email_iteration in email_list.iterrows():
                try:
                    data = {
                        "email_address" : email_iteration['Email'],
                        "status": "subscribed",
                        "merge_fields" : {
                            "TIPO" : email_iteration['TIPO'],
                            "TAG" : email_iteration['TAG']
                        }
                            
                    }
                    self.MAILCHIMP_CLIENT.lists.members.create(list_id=audience_id, data=data)
                    print('[INFO] {} HAS BEEN SUCCESSFULLY ADDED TO THE {} AUDIENCE'.format(email_iteration, audience_id))

                except Exception as e:
                    print("[INFO] IT WASN'T POSSIBLE TO ADD MEMBERS TO AUDIENCE. PLEASE CHECK LOG  🔍")    
                    f = open("automatizacion_mailchimp_log.txt", "a")
                    f.write(f'{str(e)}\n')
                    f.close()
        else: 
            print("[INFO] EMPTY LIST. PLEASE CHECK QUERY") 
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write('Nothing to send. Empty list. \n')
            f.close()


    #Funcion para crear campaña en mailchimp
    def campaign_creation_function(self, campaign_name, audience_id, subject, from_name, reply_to, template_id):

        print("[INFO] //////////////////// CREATING CAMPAING IN MAILCHIMP 🐵... ////////////////////")
            
        campaign_name = campaign_name
        audience_id = audience_id
        from_name = from_name
        reply_to = reply_to

        data = {
            "content_type": 'template',
            "recipients" :
            {
                "list_id": audience_id
            },
            "settings":
            {   
                "title": campaign_name,
                "subject_line": subject,
                "from_name": from_name,
                "reply_to": reply_to,
                'template_id': template_id,
            },
            "type": "regular"
        }
        try:
            new_campaign = self.MAILCHIMP_CLIENT.campaigns.create(data=data)
            print("[INFO] //////////////////// CAMPAING CREATED SUCCESFULLY IN MAILCHIMP 🐵... ////////////////////")
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CREATE CAMPAING. PLEASE CHECK LOG  🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return new_campaign



    #Funcion para enviar mailing
    def send_mail(self, campaign_id): 
        print("[INFO] //////////////////// SENDING CAMPAING IN MAILCHIMP 🐵... ////////////////////")    
        try:
            self.MAILCHIMP_CLIENT.campaigns.actions.send(campaign_id = campaign_id)
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO SEND CAMPAING. PLEASE CHECK LOG  🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
