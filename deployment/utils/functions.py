'-----------------------------------------1. Imports------------------------------------------------'

import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import datetime


'-----------------------------------------2. Functions------------------------------------------------'

class op_functions:

    def __init__(self):
        print("[INFO] //////////////////// MAILCHIMP AUTOMATION: SENDING ADAPTATION SURVEY 📧... ////////////////////")
    
    
    def get_date():
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


    #Funcion para obtener el listado de email para cargar en audiencia malchimp
    def get_audience_list(self, conn):

        # query para leer los registros que se deben enviar correo electronico
        query = """
        SELECT EA.[Email] 
            ,EA.[TAG]
            ,TE.[TPENC_NOMBRE] TIPO
        FROM [ODS].[dbo].[VW_ODS_Base_Apoderados_Encuesta_Adaptacion_Vigente] EA
        INNER JOIN [ODS].[dbo].[INTEGRACIONES_ZAPIER_ENC_DT_TIPO_ENCUESTA] TE ON TE.ID_TIPO_ENCUESTA = 13;
        """

        print("[INFO] //////////////////// GETTING EDUCATIONAL RESPONSIBLE LIST TO SEND ADAPTATION SURVEY 👨‍👦... ////////////////////")
        try:
            contacts = pd.read_sql_query(query, conn)
            print("[INFO] //////////////////// EDUCATIONAL RESPONSIBLE LIST OK 👨‍👦... ////////////////////")
            print(contacts.head())
        except Exception as e:
                print("[INFO] IT WASN'T POSSIBLE TO READ FROM SQL SERVER. PLEASE CHECK LOG  🔍")    
                f = open("automatizacion_mailchimp_log.txt", "a")
                f.write(f'{str(e)}\n')
                f.close()
        
        return contacts

    
    #Funcion para crear audiencia segun campos solicitados
    def audience_creation_function(self, audience_creation_dictionary, mailchimp_client):

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
            audience_creation = mailchimp_client.lists.create(data = audience_list)
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CREATE AUDIENCE. PLEASE CHECK LOG  🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return audience_creation


    #Funcion para agregar miembos a la audiencia
    def add_members_to_audience_function(self, audience_id, mail_list, mailchimp_client):
        
        print("[INFO] //////////////////// ADDING MEMBERS TO AUDIENCE IN MAILCHIMP 🙋🏻‍♀️ > 🐵... ////////////////////")
        audience_id = audience_id
        # debe ser un dataframe
        email_list = mail_list

        #merging values before to add contacts

        # merge campos de la data
        mailchimp_client.lists.merge_fields.create(list_id=audience_id, data={
            'tag': 'TAG',
            'name': 'TAG',
            'type': 'text',
            'required': False,
            'default_value': '',
            'public': True
        })


        mailchimp_client.lists.merge_fields.create(list_id=audience_id, data={
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
                    mailchimp_client.lists.members.create(list_id=audience_id, data=data)
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
    def campaign_creation_function(self, campaign_name, audience_id, subject, from_name, reply_to, template_id, mailchimp_client):

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
            new_campaign = mailchimp_client.campaigns.create(data=data)
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO CREATE CAMPAING. PLEASE CHECK LOG  🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
        
        return new_campaign



    #Funcion para enviar mailing
    def send_mail(self,campaign_id, mailchimp_client): 
        print("[INFO] //////////////////// SENDING CAMPAING IN MAILCHIMP 🐵... ////////////////////")    
        try:
            mailchimp_client.campaigns.actions.send(campaign_id = campaign_id)
        except Exception as e:
            print("[INFO] IT WASN'T POSSIBLE TO SEND CAMPAING. PLEASE CHECK LOG  🔍")    
            f = open("automatizacion_mailchimp_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()
