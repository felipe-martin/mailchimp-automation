
'-----------------------------------------1. Imports------------------------------------------------'

import pandas as pd
import numpy as np
import pyodbc
import warnings
import holidays
import requests, json
import datetime
import time
warnings.filterwarnings('ignore')


'-----------------------------------------2. Functions------------------------------------------------'


#Lectura de base de datos registros_evaluacion
def read_data_to_dataframe(endpoint_url):
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


def update_item(endpoint_url, item):

        print('[INFO] //////////// ACTUALIZANDO REGISTRO EN TABLA DYNAMODB ////////////')

        # Use the update_item method to update the item with the specified key
        try:
            response = requests.patch(endpoint_url, item)
            print('[INFO] //////////// UPDATE EJECUTADO CORRECTAMENTE ////////////')
        except Exception as e:
            print("[INFO] No fue posible hacer update en Xano")    
            f = open("videofinder_log.txt", "a")
            f.write(f'{str(e)}\n')
            f.close()

        return response


def insert_adaptation_schedulling_calendar(endpoint_url, item):
    print("[INFO] //////////// INSERTANDO REGISTRO EN TABLA DYNAMODB ////////////")
    try:
        response = requests.post(endpoint_url, item)
        print("[INFO] //////////// INSERT EJECUTADO CORRECTAMENTE ////////////")
    except Exception as e:
        print("[INFO] No fue posible hacer Insert en Xano")    
        f = open("videofinder_log.txt", "a")
        f.write(f'{str(e)}\n')
        f.close()

    return response


def get_date():
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


'-----------------------------------------3. Credentials------------------------------------------------'

#endpoint
educational_center_admissions_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:9KDs3Qon:v1/_educational_center_admissions_bulk"
monitoring_data_ingestion_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:9KDs3Qon:v1/_monitoring_data_ingestion"
adaptation_schedulling_calendar_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:9KDs3Qon:v1/_adaptation_schedulling_calendar"
current_educational_center_admissions_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:9KDs3Qon:v1/_educational_center_admissions"


'-----------------------------------------4. Conexiones------------------------------------------------'

#Timestamp fields
mailchimp_date, current_date, current_year = get_date()
#Holidays in chile
cl_holidays = holidays.country_holidays('CL', years=[current_year, current_year+1])
holiday_dates = []
for date, occasion in cl_holidays.items():
    holiday_dates.append(date)

'-----------------------------------------6. Run------------------------------------------------'

if __name__ == "__main__":
    
    current_admisions = read_data_to_dataframe(current_educational_center_admissions_endpoint)
    current_admisions['child_attendance_days_value'] = current_admisions['child_attendance_days_value'].astype(int)
    current_admisions['child_service_start_dt'] = pd.to_datetime(current_admisions['child_service_start_dt'])
    current_admisions['current_date'] = pd.to_datetime(current_date)
    current_admisions['working_days_difference'] = [np.busday_count(current_admisions['current_date'].iloc[x].date(), current_admisions['child_service_start_dt'].iloc[x].date(), holidays=holiday_dates) for x in range(current_admisions.shape[0])] 

    for i, admission in current_admisions.iterrows():
        if (admission['child_adaptation_scheduling_flag']=='false') & ((admission['working_days_difference'] <= -10) | (admission['child_attendance_days_value'] >= 6)):
            _scheduling_dt = "31/12/2050 09:00:00"
            _responsible = "No agendado"
            _comment = "Adaptación no agendada por aplicación"
            _first_interview_flag = "Sin respuesta"
            _adaptation_schedulling_flag = "wasnt scheduled"

            #Update child_adaptation_scheduling_dt, child_adaptation_responsible, child_adaptation_scheduling_comment, first_interview_flag, child_adaptation_scheduling_flag
            #in educational center admissions
            item = {"item": {
                    "child_service_id": int(admission["child_service_id"]),
                    "child_adaptation_scheduling_dt": _scheduling_dt,
                    "child_adaptation_responsible": _responsible,
                    "child_adaptation_scheduling_comment": _comment,
                    "first_interview_flag": _first_interview_flag,
                    "child_adaptation_scheduling_flag": _adaptation_schedulling_flag
                    }}
            item = json.dumps(item)
            response = update_item(current_educational_center_admissions_endpoint, item)
            
            #Insert child_adaptation_scheduling_dt, child_adaptation_responsible, child_adaptation_scheduling_comment, first_interview_flag
            #in adaptation_schedulling_calendar
            item = {"item": {
                    "child_service_id": int(admission["child_service_id"]),
                    "child_adaptation_scheduling_dt": _scheduling_dt,
                    "child_adaptation_responsible": _responsible,
                    "child_adaptation_scheduling_comment": _comment,
                    "first_interview_flag": _first_interview_flag
                    }}
            item = json.dumps(item)
            response = insert_adaptation_schedulling_calendar(adaptation_schedulling_calendar_endpoint, item)