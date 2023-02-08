'-----------------------------------------1. Imports------------------------------------------------'

import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import pyodbc
import numpy as np
import holidays
date_format = '%d/%m/%Y %H:%M:%S'


'-----------------------------------------2. Functions------------------------------------------------'

class op_functions:

    def __init__(self, mailchimp_client, sql_server_conn, dynamodb_resource, dynamodb_client, current_year):
        print("[INFO] //////////////////// MAILCHIMP AUTOMATION MODULE ACTIVE 📧... ////////////////////")
        self.SQL_SERVER_CONN = sql_server_conn
        self.MAILCHIMP_CLIENT = mailchimp_client
        self.DYNAMODB_CLIENT = dynamodb_client
        self.DYNAMODB_RESOURCE = dynamodb_resource
        cl_holidays = holidays.country_holidays('CL', years=[current_year, current_year+1])
        self.holiday_dates = []
        for date, occasion in cl_holidays.items():
            self.holiday_dates.append(date)


    #Funcion para obtener el listado de email para cargar en audiencia malchimp
    def get_tag_list(self):

        # query para leer los registros que se deben enviar correo electronico
        query = """
            SELECT 
                EA.Id_Nino_Centro child_service_id,
                EA.[Email],EA.[TAG],
                TE.[TPENC_NOMBRE] Tipo
            FROM [ODS].[dbo].[VW_ODS_Base_Apoderados_Encuesta_Adaptacion_Vigente] EA
            INNER JOIN [ODS].[dbo].[INTEGRACIONES_ZAPIER_ENC_DT_TIPO_ENCUESTA] TE ON TE.ID_TIPO_ENCUESTA = 13;
            """

        print("[INFO] //////////////////// GETTING EDUCATIONAL GUARDIAN TAG TO SEND ADAPTATION SURVEY 👨‍👦... ////////////////////")
        try:
            tags = pd.read_sql_query(query, self.SQL_SERVER_CONN)
            tags = tags.drop_duplicates(subset=['Email'])
            print("[INFO] //////////////////// EDUCATIONAL RESPONSIBLE LIST OK 👨‍👦... ////////////////////")
            print(tags.head())
        except Exception as e:
                print("[INFO] IT WASN'T POSSIBLE TO READ FROM SQL SERVER. PLEASE CHECK LOG 🔍")    
                f = open("automatizacion_mailchimp_log.txt", "a")
                f.write(f'{str(e)}\n')
                f.close()
        
        return tags

    
    #Lectura de base de datos registros_evaluacion
    def scan_dynamodb_table(self, table_name, client):
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

    
    def get_contacts(self, current_date, trigger_threshold_days, campaing_email_code):
        columns = [
        'child_service_id',
        'child_vitamina_id',
        'child_name',
        'child_educational_guardian_id',
        'child_financial_guardian_email',
        'child_educational_guardian_email',
        'child_adaptation_scheduling_dt'
        ]

        TRIGGER_THRESHOLD_DAYS = trigger_threshold_days
        
        # Lectura de base de datos adaptacion desde dynamodb.
        educational_center_admissions = self.scan_dynamodb_table('educational_center_admissions', self.DYNAMODB_CLIENT)
        # Filtrar columnas
        educational_center_admissions = educational_center_admissions[columns]
        # Filtro para quedarnos solo con adaptaciones agendadas.
        educational_center_admissions = educational_center_admissions[educational_center_admissions['child_adaptation_scheduling_dt']!=""]
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
        audience = educational_center_admissions[educational_center_admissions['send_email_flag']=='Enviar'][columns]
        audience = audience.rename(columns={'child_educational_guardian_email': 'Email' })
        #Agregar mail de prueba.
        indicator_light_email = {
            'Email': 'jaime.arroyo@vitamina.cl', 
            'TAG': '183567942-2023-02-06', 
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
