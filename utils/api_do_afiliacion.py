import requests
import pandas as pd
from datetime import datetime, timedelta
import io
import re
import time
#from utils.sheets import *

def get_users_api_consulta_afiliacion(tipo_consulta, nit_empresa=123456789):
    url_api = "https://do-api-consulta-afiliacion-empresa-880279556501.us-east1.run.app/api_consulta_afiliacion_empresa"
    print("Lectura de usuarios desde api consulta afiliación")
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
    data = {
        "nitEmpresa": nit_empresa,
        "tipoConsulta": tipo_consulta
    }
    response = requests.post(url_api, headers=headers, json=data)

    if response.status_code == 200:
        df = pd.DataFrame(response.json().get("afiliaciones", []))
        return df
    else:
        print(f"Error en la solicitud: {response.status_code}: {response.text}")
        return None