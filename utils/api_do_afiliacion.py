import requests
import pandas as pd
from datetime import datetime, timedelta
import io
import re
import time
#from utils.sheets import *

def get_users_go_integro ():
    url_api = "https://do-api-consulta-afiliacion-empresa-880279556501.us-east1.run.app/api_consulta_afiliacion_empresa"

    # realiza la solicitud GET a la API pasando como parámetro tipoConsulta = 'GO_INTEGRO' y lo almacena en la variable response
    response = requests.get(url_api, params={'tipoConsulta': 'GO_INTEGRO'})

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        # Convierte la respuesta JSON en un DataFrame de pandas y lo almacena en la variable df
        df = pd.DataFrame(response.json())
        # Retorna el DataFrame
        return df
    else:
        # Si la solicitud no fue exitosa, imprime un mensaje de error con el código de estado
        print(f"Error en la solicitud: {response.status_code}")
        return None
    

## Funcion que carga los datos en bachts a purecloud usando el microservicio 
def upload_data_pure(df,
                     contactlist, 
                     batch_size = 20):

    url_api = "https://ms-ailab-purecloud-demo-709427406268.us-east1.run.app/purecloud_upload_contact"
    # url_api = "http://0.0.0.0:8080/purecloud_upload_contact"

    batch_size = 20  # Tamaño del lote
    num_rows = len(df)
    start_index = 0

    while start_index < num_rows:
        end_index = min(start_index + batch_size, num_rows)
        batch_df = df.iloc[start_index:end_index]
        try:
            # Carga el lote a la tabla de PostgreSQL
            batch_dict = batch_df.to_dict("records")
            # print("df a dic",batch_dict)
            data_api = {"data_list_dict":batch_dict,
                        "contact_list":contactlist
                        # ,"id_contact": "ID_CASO"
                        }
            # print("requests",data_api)
            res = requests.post(url_api, json=data_api)
            if res.status_code == 200:
            # batch_df.to_sql(table_name, pool, if_exists="append", index=False)
                print(f"Cargado lote {start_index}-{end_index} correctamente.")
                msg_carga = f"Lote cargado {start_index}-{end_index} correctamente."
                msg = 'Contactos cargados con éxito - último '+ msg_carga
                res_job = {"data":res.json(),"messages": msg}
            else:
                msg = f'Error al subir los contactos: {res.status_code} - {res.text}'
                print(msg)
                print(res_job.json())
                res_job = {"data":res.json(),"messages": msg}

        except Exception as e:
            print(f"Error al cargar el lote {start_index}-{end_index}: {str(e)}")
            msg = f"Error al cargar el lote {start_index}-{end_index}: {str(e)}"
            res_job = {"data":res.json(),"messages": msg}

        start_index = end_index

    print("Carga de datos completada.")

    return res_job #s.json()

def read_file_gcs(file_path, bucket):

    blob = bucket.blob(file_path)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    return df


def execute_campaign_pure(campaignId,
                          action):
    url_api = "https://ms-ailab-purecloud-demo-709427406268.us-east1.run.app/purecloud_execution_campaign"
    # url_api = "http://0.0.0.0:8080/purecloud_execution_campaign"

    data_api = {"campaignId":campaignId,
                "action":action}
    
    print("requests",data_api)
    res = requests.post(url_api, data=data_api)
    return res


def empty_contactlist_pure(contactlist):

    url_api = "https://ms-ailab-purecloud-demo-709427406268.us-east1.run.app/purecloud_empty_contactlist"
    # url_api = "http://0.0.0.0:8080/purecloud_empty_contactlist"

    data_api = {"contactListId":contactlist}
    print("requests",data_api)
    res = requests.post(url_api, data=data_api)
    # url_api = "http://
    return res


def upload_processor(dic_contactlist_file, #listo_all_files, 
                     df_parametros,
                     bucket):
    
    # if listo_all_files == []:
    #     print(listo_all_files)
    #     date = datetime.now() - timedelta(hours=5) #.strftime("%Y%m%d")
    #     return {"messages": f"No hay archivos para cargar a la fecha {date}"}
    
    # for f in listo_all_files:
    for k, v in dic_contactlist_file.items():

        if v["files_names"] == []:
            print(v["files_names"])
            date = datetime.now() - timedelta(hours=5) #.strftime("%Y%m%d")
            print(f"No hay archivos para cargar del contactlist {k} a la fecha {date}")
            continue
        
        list_df = [read_file_gcs(file_path, bucket) for file_path in v["files_names"]]
        df_camp_conten = pd.concat(list_df)
        print(f" Tamaño de df contenido {df_camp_conten.shape}")
        
        # df_camp_conten = df_camp_conten.iloc[:1]

        # break
        
        ## Filtrar parametros por contactlist
        contactlist = k #f.split("_")[2]  #####df_parametros["ID_LISTA_CONTACTOS"].iloc[0] # 
        campaignId = v["idcampana"] 

        print("Contactlist del archivo: ", contactlist)
        df_params_camp = df_parametros[df_parametros["ID_LISTA_CONTACTOS"] == contactlist]

        ### Filtrar columnas que se van a enviar
        columns = df_params_camp["CAMPOS"].iloc[0]
        columns = re.sub(r'\s+', '', columns)
        columns = columns.split(",")

        df_camp_conten = df_camp_conten[columns]

        ### Renombrar columnas de contenido

        # df_camp_conten_columns = df_camp_conten_columns[["CONTENIDO_1","CONTENIDO_2","CONTENIDO_3","CONTENIDO_4","CONTENIDO_5"]]
        list_conten_columns = [column for column in df_params_camp.columns if column.startswith('CONTENIDO')]
        df_conten_columns = df_params_camp[list_conten_columns]
        df_conten_columns = df_conten_columns.dropna(axis=1, how='all')
        dict_conten_columns = df_conten_columns.to_dict(orient="records")

        df_camp_conten = df_camp_conten.rename(columns = dict_conten_columns[0])

        ### Renombrar columnas de OFERTA
        list_oferta_columns = [column for column in df_params_camp.columns if column.startswith('OFERTA')]
        df_oferta_columns = df_params_camp[list_oferta_columns]
        df_oferta_columns = df_oferta_columns.dropna(axis=1, how='all')
        dict_oferta_columns = df_oferta_columns.to_dict(orient="records")

        if not df_oferta_columns.empty:
            print("Existe columnas oferta")
            df_camp_conten = df_camp_conten.rename(columns = dict_oferta_columns[0])
        else:
            print("No existe columnas oferta")

        # if "95acf4e1-664f-4b3a-b8fb-0abcececa95f" in f:
        #     df_camp_conten = df_camp_conten.rename(columns={"CELULAR_1": "CELULAR",
        #                                                     "NOMBRE_DOCUMENTO": "NOMBRE_CLIENTE"})
        
        if "CELULAR" in df_camp_conten.columns:
            df_camp_conten["CELULAR"] = df_camp_conten["CELULAR"].fillna(0.0)
            df_camp_conten["CELULAR"] = df_camp_conten["CELULAR"].astype("int64")

        elif "CELULAR_1" in df_camp_conten.columns:
            df_camp_conten["CELULAR_1"] = df_camp_conten["CELULAR_1"].fillna(0.0)
            df_camp_conten["CELULAR_1"] = df_camp_conten["CELULAR_1"].astype("int64")
        
        ####
        if "CELULAR_2" in df_camp_conten.columns:
            df_camp_conten["CELULAR_2"] = df_camp_conten["CELULAR_2"].fillna(0.0)
            df_camp_conten["CELULAR_2"] = df_camp_conten["CELULAR_2"].astype("int64")
        
        ####
        if "NUMERO_DOCUMENTO" in df_camp_conten.columns:
            df_camp_conten["NUMERO_DOCUMENTO"] = df_camp_conten["NUMERO_DOCUMENTO"].fillna(0.0)
            df_camp_conten["NUMERO_DOCUMENTO"] = df_camp_conten["NUMERO_DOCUMENTO"].astype("int64")
        
        # if "CELULAR" in df_camp_conten.columns:
        #     df_camp_conten["CELULAR"] = df_camp_conten["CELULAR"].fillna(0.0)
        #     df_camp_conten["CELULAR"] = df_camp_conten["CELULAR"].astype("int64")

        # elif "CELULAR_1" in df_camp_conten.columns:
        #     df_camp_conten["CELULAR_1"] = df_camp_conten["CELULAR_1"].fillna(0.0)
        #     df_camp_conten["CELULAR_1"] = df_camp_conten["CELULAR_1"].astype("int64")

        df_camp_conten = df_camp_conten.fillna('')

        action = "stop" # or "start"
        stop = execute_campaign_pure(campaignId, action)
        print(stop.json())

        #Tiempo de descanso
        time.sleep(15)

        emp = empty_contactlist_pure(contactlist)
        print(emp.json())

        #Tiempo de descanso
        time.sleep(15)

        try:
            res_job = upload_data_pure(df_camp_conten,contactlist)
            print("Carga exitosa")
            msg = f"Carga exitosa"
            res_job = {"messages": msg}
            print(res_job)
        except Exception as e:
            print(f"Error al cargar los archivos del contactlist {contactlist}: {str(e)}")
            msg = f"Error al cargar los archivos del contactlist {contactlist}: {str(e)}"
            res_job = {"messages": msg}
        
        action = "start"
        start = execute_campaign_pure(campaignId, action)
        print(start.json())


        num_filas, num_columnas = df_camp_conten.shape
        worksheet = connect_to_sheet(sheet_id='1FAHfNfTBjH8weiHcWW4GOPA1y5ag9ryM-ESt1H1jHWU', sheet_name='purecloud')
        append_last_message_to_sheet(worksheet=worksheet, contact_list= contactlist ,total_registros= num_filas  ,total_columnas= num_columnas)
        
    return res_job

def delete_contact_pure(contactlist,
                       contact_id):

    url_api = "https://ms-ailab-purecloud-demo-709427406268.us-east1.run.app/purecloud_delete_contact"
    # url_api = "http://0.0.0.0:8080/purecloud_delete_contact"

    data_api = {"contactListId":contactlist,
                "id_contact":contact_id}
    
    # print("requests",data_api)
    res = requests.post(url_api, data=data_api)
    # url_api = "http://
    print(res.json())
    return res.json()

def delet_contact_process_pure(df_data,
                               contactlist):
    total_len = len(df_data["ID_CASO"])
    count = 0
    step = total_len // 10  # Cada 10%
    for x in df_data["ID_CASO"]:
        res = delete_contact_pure(contactlist, x)
        count += 1

        print(res)
        if count % step == 0 or count == total_len:
            # print(res)
            print(f"Progreso: {count}/{total_len} ({(count / total_len) * 100:.0f}%)")

def delete_by_contact_process(dic_contactlist_file, #listo_all_files,
                              bucket):
    
    # for f in listo_all_files:
    for k, v in dic_contactlist_file.items():

        if v["files_names"] == []:
            print(v["files_names"])
            date = datetime.now() - timedelta(hours=5) #.strftime("%Y%m%d")
            return {"messages": f"No hay archivos para cargar del contactlist {k} a la fecha {date}"}

        list_df = [read_file_gcs(file_path, bucket) for file_path in v["files_names"]]
        df_camp_conten = pd.concat(list_df)
        df_camp_conten = df_camp_conten.drop_duplicates()
        print(df_camp_conten.shape)
        
        if df_camp_conten.empty:
            print("Archivo vacío")
            continue
        print("Archivo con datos")
        # df_camp_conten = df_camp_conten.iloc[:1]

        contactlist = k #f.split("_")[2]  #####df_parametros["ID_LISTA_CONTACTOS"].iloc[0] # 
        print("Contactlist del archivo: ", contactlist)

        ## Filtrar parametros por contactlist
        # df_params_camp = df_parametros[df_parametros["ID_LISTA_CONTACTOS"] == contactlist]

        ## Delecte contactos por id
        # x_del = df_camp_conten["ID_CASO"].apply(lambda x: delete_contact_pure(contactlist,x))
        delet_contact_process_pure(df_camp_conten, 
                                   contactlist)

        return None #x_del
                              