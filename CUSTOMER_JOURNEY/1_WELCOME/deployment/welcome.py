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
# Inicializar modulo de configuracion
cfg = Config()

# Conectar aplicaciones 🧰
MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD, ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4, ENDPOINT_5, ENDPOINT_6 = cfg.get_credentials()
mailchimp_client, sql_server_conn = cfg.connect_tools(MAILCHIMP_USERNAME, MAILCHIMP_APIKEY, SQL_SERVER, SQL_SERVER_DATABASE, SQL_SERVER_USERNAME, SQL_SERVER_PASSWORD)

#sql server
server = "10.10.5.7,1433" 
database = "vitamina" 
username = "consulta" 
password = "consulta1" 

# Obtener fechas en distintos formatos
mailchimp_date, current_date, current_year = cfg.get_date()

# Obteniendo configuracion de la campaña especifica
campaing_email_code = args.campaing_email_code
email_template, email_from_name, email_reply_to, email_subject, email_send_flag, trigger_threshold_days = cfg.get_config_template(campaing_email_code)
threshold_days = trigger_threshold_days * -1

print("Cantidad de dias a observar: ", threshold_days)
print("Asunto de la campaña: ", email_subject)

fn = op_functions(mailchimp_client, sql_server_conn, current_year, ENDPOINT_1, ENDPOINT_2, ENDPOINT_3, ENDPOINT_4, ENDPOINT_5, ENDPOINT_6)

# Sql server
cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER="+server+";DATABASE="+database+";UID="+username+";PWD="+password)
cursor = cnxn.cursor()


"-----------------------------------------5. Query------------------------------------------------"

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
    "Id_Nino":"child_vitamina_id", 
    "Id_Nino_Centro":"child_service_id", 
    "Nombre_Completo_Nino":"child_name",
    "Rut_Nino":"child_dni_code",
    "Fecha_Nacimiento_Nino":"child_date_birth",
    "Genero_Nino":"child_gender_desc",
    "Id_Apoderado_Financiero":"child_financial_guardian_id",
    "Nombre_Completo_Apoderado_Financiero":"child_financial_guardian_name",
    "Rut_Apoderado_Financiero":"child_financial_guardian_dni_code",
    "Relacion_Apoderado_Financiero":"child_financial_guardian_relationship_desc", 
    "Email_Apoderado_Financiero":"child_financial_guardian_email",
    "Id_Apoderado_Educativo":"child_educational_guardian_id", 
    "Nombre_Completo_Apoderado_Educativo":"child_educational_guardian_name",
    "Rut_Apoderado_Educativo":"child_educational_guardian_dni_code", 
    "Relacion_Apoderado_Educativo":"child_educational_guardian_relationship_desc",
    "Email_Apoderado_Educativo":"child_educational_guardian_email", 
    "Id_Centro":"educational_center_code", 
    "Nombre_Centro":"educational_center_name", 
    "Canal":"child_admission_channel_value",
    "Nivel":"child_level_name", 
    "Jornada":"child_service_type_desc", 
    "Id_Sala":"educational_center_room_id", 
    "Fecha_Ultima_Matricula":"child_last_enrollment_dt",
    "Fecha_Ultima_Renovacion_Matricula":"child_last_renewal_process_dt",
    "Fecha_Inicio_Servicio":"child_service_start_dt",
    "Fecha_Fin_Servicio":"child_service_end_dt", 
    "Estado_Servicio":"child_service_status",
    "Dias_Asistidos":"child_attendance_days_value"
}

"-----------------------------------------6. Run------------------------------------------------"

if __name__ == "__main__":

    # Lectura base desde SQL Server VTM
    new_admissions = pd.read_sql_query(query, cnxn)
    new_admissions = new_admissions.rename(columns=columns_rename_dict)
    
    # Obteniendo nuevos ingresos para enviar mail de bienvenida
    contacts = fn.get_new_admissions(new_admissions, current_date, threshold_days, campaing_email_code) 
    print("Contactos a cargar: \n", contacts.shape[0])
    # Cargando nuevos ingresos en audiencia para activar journey en Mailchimp
    fn.add_members_to_welcome_journey('9b5c022126', contacts)
    print("[INFO] //////////// PROCESO EJECUTADO CORRECTAMENTE 🤘🏻 ////////////")
