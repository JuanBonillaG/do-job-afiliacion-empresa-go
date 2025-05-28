import requests
import pandas as pd

from google.cloud import secretmanager
#from utils.sheets import *
    
def get_api_token():
    """
    Obtiene el access_token desde el endpoint de GO Integro.

    Returns:
        str: access_token obtenido o None si falla la solicitud.
    """

    client = secretmanager.SecretManagerServiceClient()
    url = client.access_secret_version(name='projects/287760044484/secrets/go_token_url/versions/latest').payload.data.decode("UTF-8") + "/oauth/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client.access_secret_version(name='projects/287760044484/secrets/go_client_id/versions/latest').payload.data.decode("UTF-8"),
        "client_secret": client.access_secret_version(name='projects/287760044484/secrets/go_client_secret/versions/latest').payload.data.decode("UTF-8")
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            result = response.json()
            return result.get("access_token")
        else:
            print(f"Error en acceso: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        print(f"Excepción en acceso: {e}")
        return None

def get_users_api_go_integro(token):
    """
    Obtiene la lista de usuarios desde el endpoint de GO Integro.

    Args:
        token (str): access_token para autorización.

    Returns:
        dataframe: Respuesta del endpoint como dataframe, o None si falla la solicitud.
    """
    client = secretmanager.SecretManagerServiceClient()
    url = client.access_secret_version(name='projects/287760044484/secrets/go_token_url/versions/latest').payload.data.decode("UTF-8") + "/users"
    headers = {
        "accept": "application/json",
        "Authorization": token
    }
    print("Lectura de de usuarios desde API GO Integro")
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])

            users = []
            for user in data:
                attributes = user.get("attributes", {})
                users.append({
                    "employee_id": user.get("id"),
                    "email": attributes.get("email"),
                    "first_name": attributes.get("name"),
                    "last_name": attributes.get("last-name"),
                    "document_type": attributes.get("document-type"),
                    "document": attributes.get("document"),
                    "external_id": attributes.get("external-id")
                })

            return pd.DataFrame(users)
        else:
            print(f"Error al obtener usuarios: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        print(f"Excepción al obtener usuarios: {e}")
        return None

import requests
import json

def create_users_api_go_integro(df, token):
    """
    Obtiene la lista de usuarios desde el endpoint de GO Integro.

    Args:
        token (str):    access_token para autorización.
        df(dataframe):  dataframe con usuarios por crear.

    Returns:
        str: Respuesta del endpoint exitoso o lista de fallidos.
    """
    client = secretmanager.SecretManagerServiceClient()
    url = client.access_secret_version(name='projects/287760044484/secrets/go_token_url/versions/latest').payload.data.decode("UTF-8") + "/users"
    headers = {
        "accept": "application/json",
        "Authorization": token,
        "Content-Type": "application/json"
    }

    failed_users = []

    for index, row in df.iterrows():
        payload = {
            "data": {
                "type": "users",
                "attributes": {
                    "name": row["first_name"],
                    "last-name": row["last_name"],
                    "email": row["email"],
                    "employee-id": row["employee_id"],
                    "document-type": row["document_type"],
                    "document": row["document"],
                    "external_id": row["external_id"],
                    "status": "active",
                    "login-enabled": True
                }
            }
        }

        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 201:
                print(f"Usuario {row['employee_id']} creado correctamente.")
            else:
                print(f"Error al crear usuario {row['employee_id']}: {response.status_code}, {response.text}")
                failed_users.append(row["employee_id"])
        except Exception as e:
            print(f"Excepción al crear usuario {row['employee_id']}: {e}")
            failed_users.append(row["employee_id"])
    
    if failed_users:
        return f"usuarios fallidos: {failed_users}"
    else:
        return "exitoso"