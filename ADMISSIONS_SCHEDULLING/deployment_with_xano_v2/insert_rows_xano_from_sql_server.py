
'-----------------------------------------1. Imports------------------------------------------------'
import requests, json
import numpy as np
import pandas as pd
import pyodbc
import warnings
import datetime
import holidays
warnings.filterwarnings("ignore")
from tqdm import tqdm
import time


"-----------------------------------------2. Functions------------------------------------------------"

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
    current_date = f"{d}/{M}/{Y} {h}:{m}:{s}"
    
    return current_date


def insert_monitoring_data_ingestion(endpoint_url, current_date, registers_no):
    print("[INFO] //////////// INSERTANDO REGISTRO EN TABLA DYNAMODB ////////////")
    # Create the item to be inserted
    item = {"item": {
        "data_ingestion_process_dt": f"{current_date}",
        "loaded_rows_value": registers_no
        }}
    item = json.dumps(item)
    print("[INFO] //////////// INSERT EJECUTADO CORRECTAMENTE ////////////")

    # Insert the item into the DynamoDB table
    response = requests.post(endpoint_url, data=item)
    
    return response


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


def post_dataframe_rows_bulk(endpoint_url, dataframe, batch_size):

    # Iterate over the dataframe in chunks of 300 rows
    for i in tqdm(range(0, len(dataframe), batch_size), "Loading Xano from SQL Server:"):
        items = []
        for j, row in dataframe[i:i + batch_size].iterrows():
            item = {
            "child_service_id": int(row["child_service_id"]),
            "child_adaptation_scheduling_flag": str(row["child_adaptation_scheduling_flag"]),
            "child_adaptation_scheduling_start_dt": str(row["child_adaptation_scheduling_start_dt"]),
            "child_adaptation_scheduling_comment": str(row["child_adaptation_scheduling_comment"]),
            "child_adaptation_responsible": str(row["child_adaptation_responsible"]),
            "child_admission_channel_value": str(row["child_admission_channel_value"]),
            "child_date_birth": str(row["child_date_birth"]),
            "child_dni_code": int(row["child_dni_code"]),
            "child_educational_guardian_dni_code": int(row["child_educational_guardian_dni_code"]),
            "child_educational_guardian_email": str(row["child_educational_guardian_email"]),
            "child_educational_guardian_id": int(row["child_educational_guardian_id"]),
            "child_educational_guardian_name": str(row["child_educational_guardian_name"]),
            "child_educational_guardian_relationship_desc":  str(row["child_educational_guardian_relationship_desc"]),
            "child_financial_guardian_dni_code": int(row["child_financial_guardian_dni_code"]),
            "child_financial_guardian_email":  str(row["child_financial_guardian_email"]),
            "child_financial_guardian_id":  int(row["child_financial_guardian_id"]),
            "child_financial_guardian_name":  str(row["child_financial_guardian_name"]),
            "child_financial_guardian_relationship_desc":  str(row["child_financial_guardian_relationship_desc"]),
            "child_gender_desc":  str(row["child_gender_desc"]),
            "child_last_enrollment_dt":  str(row["child_last_enrollment_dt"]),
            "child_last_renewal_process_dt":  str(row["child_last_renewal_process_dt"]),
            "child_level_name":  str(row["child_level_name"]),
            "child_name":  str(row["child_name"]),
            "child_service_start_dt":  str(row["child_service_start_dt"]),
            "child_service_status":  str(row["child_service_status"]),
            "child_service_type_desc":  str(row["child_service_type_desc"]),
            "child_vitamina_id":  int(row["child_vitamina_id"]),
            "educational_center_code":  str(row["educational_center_code"]),
            "educational_center_name":  str(row["educational_center_name"]),
            "educational_center_room_id":  int(row["educational_center_room_id"]),
            "child_attendance_days_value": int(row["child_attendance_days_value"]),
            "flag_poc_center": str(row["flag_poc_center"]),
            "interview_flag": str(row["interview_flag"]),
            "child_interview_start_dt": str(row['child_interview_start_dt']),
            "child_interview_responsible": str(row['child_interview_responsible']),
            "child_interview_end_dt": str(row['child_interview_end_dt']),
            "scheduling_channel_value": str(row['scheduling_channel_value']),
            "child_adaptation_scheduling_end_dt": str(row['child_adaptation_scheduling_end_dt']),
            "center_interview_responsible": str(row["center_interview_responsible"])


            }
            items.append(item)
        json_array = f'"items" : {items}'
        json_array = "{" + json_array + "}"
        json_array = json_array.replace("'",'"')
        data = json.loads(json_array)
        
          
        try:
            response = requests.post(endpoint_url, json=data)
            #print(f"[INFO] //////////// BATCH EJECUTADO CORRECTAMENTE ////////////")
        except Exception as e:
            print("//////////// ERROR EN EJECUCION DE BATCH. REVISAR LOG ////////////")    
            f = open("admissions_adaptation_schedulling.txt", "a")
            f.write(f"{str(e)}\n")
            f.close()
    print(f"[INFO] //////////// BATCH EJECUTADO CORRECTAMENTE ////////////")
        

'-----------------------------------------3. Credentials------------------------------------------------'

#sql server
server = "10.10.5.7,1433" 
database = "vitamina" 
username = "consulta" 
password = "consulta1" 

#endpoint
educational_center_admissions_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:Oq7cztlT/v2_educational_center_admissions_bulk"
adaptation_schedulling_calendar_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:9KDs3Qon/v2_adaptation_schedulling_calendar"
current_educational_center_admissions_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:9KDs3Qon/v2_educational_center_admissions"
monitoring_data_ingestion_endpoint = "https://xmmk-gpob-yjqm.n7.xano.io/api:Oq7cztlT/v2_monitoring_data_ingestion"


"-----------------------------------------4. Conexiones------------------------------------------------"

#sql server
cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+password)
cursor = cnxn.cursor()


'-----------------------------------------5. Query------------------------------------------------'

query = """ 
WITH Renovacion AS
(
SELECT HA.[id_nino]
      ,HA.[HSACC_Periodo] Año
      ,min(HA.[fecha_accion]) FechaRen
FROM [Vitamina].[dbo].[Historial_Acciones] HA
WHERE id_accion=15 AND HSACC_Periodo = year(getdate()) AND convert(varchar(6),fecha_accion,112) BETWEEN cast(HSACC_Periodo-1 as varchar)+'07' AND cast(HSACC_Periodo as varchar)+'02'
AND EXISTS (
            SELECT 1 FROM [Vitamina].[dbo].[nino_centro] NCaux2
            WHERE HA.[HSACC_Periodo]-1 between year(NCaux2.fecha_ingreso) and year(isnull(NCaux2.fecha_retiro,'30000101'))
            AND HA.id_nino = NCaux2.id_nino and ( datediff(d,NCaux2.fecha_ingreso,NCaux2.fecha_retiro) > 0 or NCaux2.fecha_retiro is null)
            )
GROUP BY id_nino,HSACC_Periodo
)
, Conversion AS
(
SELECT NC.id_nino
         ,convert(VARCHAR(6), getdate(), 112) Periodo
         ,C.ModifyDate FechaConv
         ,NC.id_ninocentro
         ,NC.fecha_ingreso
         ,ROW_NUMBER() OVER (PARTITION BY NC.id_nino ORDER BY C.ModifyDate desc) Fila
FROM [Vitamina].[dbo].[nino_centro] NC
INNER JOIN [Vitamina].[dbo].[Cotizacion] C on C.id_cotizacion = NC.id_cotizacion
WHERE convert(varchar(6),NC.fecha_ingreso,112) <= convert(VARCHAR(6), getdate(), 112)
)
 
 
SELECT NC.Id_Nino,
       NC.Id_NinoCentro Id_Nino_Centro,
       rtrim(N.nom_nino) + ' ' + rtrim(N.app_nino) + case when N.apm_nino <> '' then ' ' + rtrim(N.apm_nino) else '' end Nombre_Completo_Nino,
       N.Rut_Nino,
       N.fechanac_nino Fecha_Nacimiento_Nino,
       N.sexo Genero_Nino,
       PF.Id_Persona Id_Apoderado_Financiero,
       rtrim(PF.nom_p) + ' ' + rtrim(PF.app_p) + case when PF.apm_p <> '' then ' ' + rtrim(PF.apm_p) else '' end Nombre_Completo_Apoderado_Financiero,
       PF.rut_p Rut_Apoderado_Financiero,
       rtrim(RelF.desc_relacion) Relacion_Apoderado_Financiero,
       rtrim(lower(PF.email_p)) Email_Apoderado_Financiero,
       PE.Id_Persona Id_Apoderado_Educativo,
       rtrim(PE.nom_p) + ' ' + rtrim(PE.app_p) + case when PE.apm_p <> '' then ' ' + rtrim(PE.apm_p) else '' end Nombre_Completo_Apoderado_Educativo,
       PE.rut_p Rut_Apoderado_Educativo,
       rtrim(RelE.desc_relacion) Relacion_Apoderado_Educativo,
       rtrim(lower(PE.email_p)) Email_Apoderado_Educativo,
       rtrim(NC.Id_Centro) Id_Centro,
       rtrim(CE.desc_centro) Nombre_Centro,
       case when NFE.id_ninocentro IS NOT NULL then 'Empresa' else 'Familia' end Canal,
       rtrim(Niv.desc_nivel) Nivel,
       rtrim(J.desc_jornada) Jornada,
       Prod.Id_Sala,
       isnull(C.ModifyDate,isnull(CV.FechaConv,NC.fecha_ingreso)) Fecha_Ultima_Matricula,
       case when NFE.id_ninocentro IS NULL
            then case when isnull(Ren.id_nino,R.id_nino) is not null
                        then isnull(Ren.Fecha_Renovacion,R.FechaRen)
                        when isnull(C.ModifyDate,CV.FechaConv) < convert(VARCHAR(4),getdate(),112)+'0101'
                        then isnull(C.ModifyDate,CV.FechaConv)
                        else NULL
                        end
            else case when isnull(C.ModifyDate,isnull(CV.FechaConv,'20000101')) < convert(VARCHAR(4),getdate(),112)+'0101'
                        then convert(VARCHAR(4),getdate(),112)+'0101'
                        when convert(varchar(6),isnull(C.ModifyDate,CV.FechaConv),112) = convert(VARCHAR(4),getdate(),112)+'01'
                            AND EXISTS (
                                                                   SELECT 1 FROM [Vitamina].[dbo].[nino_centro] NCaux2
                                                                   WHERE year(getdate())-1 between year(NCaux2.fecha_ingreso) and year(isnull(NCaux2.fecha_retiro,'30000101'))
                                                                   AND NC.id_nino = NCaux2.id_nino and ( datediff(d,NCaux2.fecha_ingreso,NCaux2.fecha_retiro) > 0 or NCaux2.fecha_retiro is null)
                                                                   )
                        then isnull(C.ModifyDate,CV.FechaConv)
                        else NULL
                        end
            end Fecha_Ultima_Renovacion_Matricula,
       NC.[fecha_ingreso] Fecha_Inicio_Servicio,
       NC.[fecha_retiro] Fecha_Fin_Servicio,
       E.desc_estado Estado_Servicio,
          isnull(A.Dias_Asistidos,0) Dias_Asistidos
FROM [Vitamina].[dbo].[nino_centro] NC
INNER JOIN (
       SELECT id_nino,
             max(isnull(fecha_retiro, '30000101')) max_fecha_retiro
       FROM [Vitamina].[dbo].[nino_centro]
       WHERE datediff(d, fecha_ingreso, isnull(fecha_retiro, '30000101')) > 0
       AND (
             fecha_retiro >= getdate()
             OR fecha_retiro IS NULL
             )
       AND convert(VARCHAR(6), fecha_ingreso, 112) <= convert(VARCHAR(6), getdate()+14, 112)
       GROUP BY id_nino
       ) NC5 ON NC.id_nino = NC5.id_nino AND isnull(NC.fecha_retiro, '30000101') = NC5.max_fecha_retiro -- traemos el último servicio vigente (activo o matriculado) de cada niño, no más allá de 1 mes hacia adelante
LEFT JOIN [Vitamina].[dbo].[Nino] N ON NC.id_nino = N.id_nino
LEFT JOIN [Vitamina].[dbo].[Persona_Nino_Pago] PNP ON PNP.id_nino = N.id_nino
LEFT JOIN [Vitamina].[dbo].[Persona] PF ON PF.id_persona = PNP.id_persona
LEFT JOIN [Vitamina].[dbo].[Familia_Persona] FPF ON FPF.id_familia = N.id_familia AND FPF.id_persona = PNP.id_persona
LEFT JOIN [Vitamina].[dbo].[Relacion] RelF ON RelF.id_relacion = FPF.id_relacion
LEFT JOIN [Vitamina].[dbo].[Persona] PE ON PE.id_persona = N.id_persona
LEFT JOIN [Vitamina].[dbo].[Familia_Persona] FPE ON FPE.id_familia = N.id_familia AND FPE.id_persona = N.id_persona
LEFT JOIN [Vitamina].[dbo].[Relacion] RelE ON RelE.id_relacion = FPE.id_relacion
LEFT JOIN [Vitamina].[dbo].[Estados_Nino] E ON E.id_estado = NC.id_estado
LEFT JOIN [Vitamina].[dbo].[Centro] CE ON CE.id_centro = NC.id_centro
LEFT JOIN [Vitamina].[dbo].[Producto_Centro] Prod ON Prod.Id_ProdCentro = NC.id_producto_centro
LEFT JOIN [Vitamina].[dbo].[Nivel] Niv ON Niv.id_nivel = Prod.id_nivel
LEFT JOIN [Vitamina].[dbo].[Jornada] J ON J.id_jornada = Prod.id_jornada
LEFT JOIN [Vitamina].[dbo].[Nino_SucursalEmpresa] NFE ON NFE.id_ninocentro = NC.id_ninocentro
LEFT JOIN [Vitamina].[dbo].[Cotizacion] C ON C.id_cotizacion = NC.id_cotizacion
LEFT JOIN [SRV_SQL_BI].[VitaGestion].[dbo].[Z_Renovados] Ren on Ren.id_nino = NC.id_nino and Ren.Año = year(getdate())-1
LEFT JOIN Renovacion R ON R.id_nino = NC.id_nino --and R.Año = left(Q.ZQ_Periodo,4)
LEFT JOIN Conversion CV ON CV.Fila=1 and CV.id_nino = NC.id_nino --and CV.Periodo = Q.ZQ_Periodo
LEFT JOIN ( SELECT [id_ninocentro]
                               ,sum(case when ID_ASISTENCIA_DIURNO=1 or ID_ASISTENCIA_RETAIL=1 or ID_ASISTENCIA_NOCTURNO=1 then 1 else 0 end) Dias_Asistidos
                      FROM [Vitamina].[dbo].[VTMN_FT_ASISTENCIA_NINO]
                      GROUP BY [id_ninocentro]
  ) A ON A.id_ninocentro = NC.id_ninocentro
WHERE datediff(d, NC.fecha_ingreso, isnull(NC.fecha_retiro, '30000101')) > 0 --sacamos anulados
       AND NC.fecha_ingreso >= convert(varchar(8),dateadd(d,-10,getdate()),112) --traemos ingresos desde 10 días hacia atrás
       AND (
             NC.fecha_retiro >= DATEADD(mm, 1, convert(varchar(6),NC.fecha_ingreso+14,112) + '01') --sacamos retiros tempranos
             OR NC.fecha_retiro IS NULL
             )
       AND NOT EXISTS ( --validamos que sean ingresos
                        SELECT 1 FROM [Vitamina].[dbo].[nino_centro] NCaux
                        WHERE convert(VARCHAR(6), dateadd(m, -1, NC.fecha_ingreso), 112) <= convert(VARCHAR(6),isnull(NCaux.fecha_retiro,'30000101'),112) AND NCaux.fecha_ingreso < NC.fecha_ingreso
                        AND NC.id_nino = NCaux.id_nino AND NC.id_ninocentro <> NCaux.id_ninocentro and ( datediff(d,NCaux.fecha_ingreso,NCaux.fecha_retiro) > 0 or NCaux.fecha_retiro is null)
                      )
"""

columns_rename_dict = {
    'Id_Nino':'child_vitamina_id', 
    'Id_Nino_Centro':'child_service_id', 
    'Nombre_Completo_Nino':'child_name',
    'Rut_Nino':'child_dni_code',
    'Fecha_Nacimiento_Nino':'child_date_birth',
    'Genero_Nino':'child_gender_desc',
    'Id_Apoderado_Financiero':'child_financial_guardian_id',
    'Nombre_Completo_Apoderado_Financiero':'child_financial_guardian_name',
    'Rut_Apoderado_Financiero':'child_financial_guardian_dni_code',
    'Relacion_Apoderado_Financiero':'child_financial_guardian_relationship_desc', 
    'Email_Apoderado_Financiero':'child_financial_guardian_email',
    'Id_Apoderado_Educativo':'child_educational_guardian_id', 
    'Nombre_Completo_Apoderado_Educativo':'child_educational_guardian_name',
    'Rut_Apoderado_Educativo':'child_educational_guardian_dni_code', 
    'Relacion_Apoderado_Educativo':'child_educational_guardian_relationship_desc',
    'Email_Apoderado_Educativo':'child_educational_guardian_email', 
    'Id_Centro':'educational_center_code', 
    'Nombre_Centro':'educational_center_name', 
    'Canal':'child_admission_channel_value',
    'Nivel':'child_level_name', 
    'Jornada':'child_service_type_desc', 
    'Id_Sala':'educational_center_room_id', 
    'Fecha_Ultima_Matricula':'child_last_enrollment_dt',
    'Fecha_Ultima_Renovacion_Matricula':'child_last_renewal_process_dt',
    'Fecha_Inicio_Servicio':'child_service_start_dt',
    'Fecha_Fin_Servicio':'child_service_end_dt', 
    'Estado_Servicio':'child_service_status',
    'Dias_Asistidos':'child_attendance_days_value'
}

'-----------------------------------------6. Run------------------------------------------------'

if __name__ == "__main__":
    #Lectura de tabla de calendario de adaptaciones agendadas 
    #current_adaptation_scheduled = read_data_to_dataframe(adaptation_schedulling_calendar_endpoint)
    #current_adaptation_scheduled['child_service_id'] = current_adaptation_scheduled['child_service_id'].astype(int)
    #print(f"[INFO] //////////// TOTAL AGENDAMIENTOS REALIZADOS: {len(current_adaptation_scheduled)} REGISTROS")
    print(f"[INFO] //////////// INICIANDO PROCESO DE INGESTA DE NUEVOS INGRESOS VITAMINA ////////////")
    #Carga base desde SQL Server VTM
    admissions = pd.read_sql_query(query, cnxn)
    admissions = admissions.rename(columns=columns_rename_dict)
    #admissions = admissions.merge(current_adaptation_scheduled, how='left', on='child_service_id')
    admissions['child_vitamina_id'] = admissions['child_vitamina_id'].astype(int) 
    
    #Valores por omision al momento de la carga. Dada la construccion de la API, si el valor existe, no modifica estos valores.
    admissions['child_adaptation_scheduling_flag'] = "false"
    admissions['child_adaptation_scheduling_comment'] = "Sin comentarios"
    admissions['child_adaptation_scheduling_start_dt'] = ""
    admissions['child_adaptation_responsible'] = ""
    admissions['interview_flag'] = "false"
    admissions['child_interview_start_dt'] = ""
    admissions['child_interview_responsible'] = ""
    admissions['child_interview_end_dt'] = ""
    admissions['scheduling_channel_value'] = ""
    admissions['child_adaptation_scheduling_end_dt'] = ""
    admissions['center_interview_responsible'] = ""
    admissions['flag_poc_center'] = np.where(admissions['educational_center_code'].isin(['NPL2','QLC','SMH','CCBA']), "true", "false")


    #Mejora pendiente. Calcular ratio de elementos subidos vs por subir.

    print(f"[INFO] //////////// REGISTROS LEIDOS EXITOSAMENTE. SE SUBIRAN {admissions.shape[0]} FILAS")

    #insertar registros en base de datos Xano
    response = post_dataframe_rows_bulk(educational_center_admissions_endpoint, admissions, 300)
    f = open("admissions_adaptation_schedulling.txt", "a")
    f.write(f"{str(response)}\n")
    f.close()

    current_date = get_date()
    no_rows = admissions.shape[0]
    #Guardar registro de actualizacion
    response = insert_monitoring_data_ingestion(monitoring_data_ingestion_endpoint, current_date, no_rows)
    f = open("data_ingestion_log.txt", "a")
    f.write(f"{str(response)}\n")
    f.close()
