import requests
import pandas as pd
import json

from google.cloud import secretmanager
    
def get_api_token():
    """
    Obtiene el access_token desde el endpoint de GO Integro.

    Returns:
        str: access_token obtenido o None si falla la solicitud.
    """

    client = secretmanager.SecretManagerServiceClient()
    url ="https://api.gointegro.com/oauth/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client.access_secret_version(name='projects/263751840195/secrets/api_afiliacion_empresa-go_client_id/versions/latest').payload.data.decode("UTF-8"),
        "client_secret": client.access_secret_version(name='projects/263751840195/secrets/api_afiliacion_empresa-go_client_secret/versions/latest').payload.data.decode("UTF-8")
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
    Obtiene la lista completa de usuarios desde el endpoint de GO Integro con paginación.

    Args:
        token (str): access_token para autorización.

    Returns:
        DataFrame: Respuesta del endpoint como DataFrame, o None si falla la solicitud.
    """
    print("Lectura de usuarios desde API GO Integro")
    base_url = "https://api.gointegro.com/users"
    headers = {
        "accept": "application/json",
        "Authorization": token
    }

    page_size = 50
    page_number = 1
    all_users = []

    try:
        while True:
            params = {
                "page[size]": page_size,
                "page[number]": page_number
            }

            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error al obtener usuarios: {response.status_code}, {response.text}")
                return None

            json_response = response.json()

            data = json_response.get("data", [])
            for user in data:
                attributes = user.get("attributes", {})
                all_users.append({
                    "id": user.get("id"),
                    "employee_id": attributes.get("employee-id"),
                    "email": attributes.get("email"),
                    "first_name": attributes.get("name"),
                    "last_name": attributes.get("last-name"),
                    "document_type": attributes.get("document-type"),
                    "document": attributes.get("document"),
                    "external_id": attributes.get("external-id")
                })

            # Validación de proceso de Paginación
            pagination = json_response.get("meta", {}).get("pagination", {})
            total_pages = pagination.get("total-pages", 1)
            if page_number >= total_pages:
                break
            page_number += 1

        return pd.DataFrame(all_users)

    except Exception as e:
        print(f"Excepción al obtener usuarios: {e}")
        return None

def create_users_api_go_integro(df, token):
    """
    Obtiene la lista de usuarios desde el endpoint de GO Integro.

    Args:
        token (str):    access_token para autorización.
        df(dataframe):  dataframe con usuarios por crear.

    Returns:
        str: Respuesta del endpoint exitoso o lista de fallidos.
    """
    url ="https://api.gointegro.com/users"
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
                },
                "relationships": {
                    "group-items": {
                        "data": [
                            {
                                "type": "group-items",
                                "id": row["group_item_id"]
                            }
                        ]
                    }
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
    
def update_users_api_go_integro(df, token):
    """
    Actualiza usuarios en GO Integro.

    Args:
        token (str):    access_token para autorización.
        df (DataFrame): DataFrame con los usuarios a actualizar.

    Returns:
        list: Lista con mensajes de respuesta por cada usuario.
    """
    results = []

    for idx, row in df.iterrows():
        user_id = row["id"]
        url = f"https://api.gointegro.com/users/{user_id}"
        headers = {
            "accept": "application/json",
            "Authorization": token,
            "Content-Type": "application/json"
        }
        payload = {
            "data": {
                "type": "users",
                "id": user_id,
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
                },
                "relationships": {
                    "group-items": {
                        "data": [
                            {
                                "type": "group-items",
                                "id": row["group_item_id"]
                            }
                        ]
                    }
                }
            }
        }
        try:
            response = requests.patch(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                results.append(f"Usuario {user_id},{row["document"]} actualizado correctamente.")
            else:
                results.append(f"Error al actualizar usuario {user_id},{row["document"]}: {response.status_code}, {response.text}")
        except Exception as e:
            results.append(f"Excepción al actualizar usuario {user_id},{row["document"]}: {e}")

    return results

def get_group_items_api_go_integro(token):
    """
    Obtiene la lista completa de grupos desde el endpoint de GO Integro con paginación.

    Args:
        token (str): access_token para autorización.

    Returns:
        DataFrame: Respuesta del endpoint como DataFrame, o None si falla la solicitud.
    """
    print("Lectura de usuarios desde API GO Integro")
    base_url = "https://api.gointegro.com/group-items"
    headers = {
        "accept": "application/json",
        "Authorization": token
    }

    page_size = 50
    page_number = 1
    all_group_items = []

    try:
        while True:
            params = {
                "page[size]": page_size,
                "page[number]": page_number
            }

            response = requests.get(base_url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error al obtener group items: {response.status_code}, {response.text}")
                return None

            json_response = response.json()

            data = json_response.get("data", [])
            for group_item in data:
                attributes = group_item.get("attributes", {})
                all_group_items.append({
                    "id": group_item.get("id"),
                    "type": group_item.get("type"),
                    "name": attributes.get("name")
                })

            # Validación de proceso de Paginación
            pagination = json_response.get("meta", {}).get("pagination", {})
            total_pages = pagination.get("total-pages", 1)
            if page_number >= total_pages:
                break
            page_number += 1

        return pd.DataFrame(all_group_items)

    except Exception as e:
        print(f"Excepción al obtener group items: {e}")
        return None