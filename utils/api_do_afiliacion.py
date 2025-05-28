import requests
import pandas as pd
from datetime import datetime, timedelta
import io
import re
import time
#from utils.sheets import *

def get_users_api_consulta_afiliacion (tipo_consulta):
    url_api = "https://do-api-consulta-afiliacion-empresa-880279556501.us-east1.run.app/api_consulta_afiliacion_empresa"
    print("Lectura de usuarios desde api consulta afiliación")
    # realiza la solicitud GET a la API pasando como parámetro tipoConsulta = 'GO_INTEGRO' y lo almacena en la variable response
    response = requests.get(url_api, params={'tipoConsulta': tipo_consulta})

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        # Convierte la respuesta JSON en un DataFrame de pandas y lo almacena en la variable df
        df = pd.DataFrame(response.json().get("afiliaciones",[]))
        # Retorna el DataFrame
        return df
    else:
        # Si la solicitud no fue exitosa, imprime un mensaje de error con el código de estado
        print(f"Error en la solicitud: {response.status_code}")
        return None