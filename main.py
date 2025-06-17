import pandas as pd

from utils.api_do_afiliacion import get_users_api_consulta_afiliacion
from utils.api_go_integro import get_users_api_go_integro, get_api_token, get_group_items_api_go_integro,create_users_api_go_integro, update_users_api_go_integro
from utils.job_functions import compare_users, update_users_with_group_items

## Lee los usuarios desde la API Afiliación de empresas
df_users = get_users_api_consulta_afiliacion('GO_INTEGRO')

# Mock de respuesta de api consulta afiliación
df_users = pd.DataFrame([{
    "first_name": "Antiguo Usuario",
    "email": "pruebaactualizado@yopmail.com",
    "last_name": "Actualizado job",
    "employee_id": None,
    "document_type": "CC",
    "groups": "Empresas A&A:COLOMBIA SAS",
    "document": 12345,
    "external_id": "SB-987654321"
},
{
    "first_name": "Nuevo Usuario",
    "email": None,
    "last_name": "Creado job",
    "employee_id": None,
    "document_type": "CC",
    "groups": "GOintegro:Recursos Humanos",
    "document": 34567,
    "external_id": "SB-4389278"
}])

# Valida que existan usuarios en las empresas registradas
if df_users is None or df_users.empty:
    print("No se encontraron usuarios en la API consulta afiliación empresa")
    exit()  

print(f"Cantidad de usuarios encontrados en la API consulta afiliación empresa: {len(df_users)}")

## Obtiene token de acceso GO INTEGRO
token = get_api_token()

## Lee los usuarios y grupos(empresas) ya registrados en GO INTEGRO
df_users_go = get_users_api_go_integro(token)
df_group_items = get_group_items_api_go_integro(token)

# Asegura la consistencia de tipos
df_users["document"] = df_users["document"].astype(str)
df_users_go["document"] = df_users_go["document"].astype(str)

## 1. Filtra usuarios cuyo documento no existe en GO INTEGRO
df_new_users = df_users[~df_users["document"].isin(df_users_go["document"])]
result        = 'exitoso'
result_update = 'exitoso'

if not df_new_users.empty:
    print(f"Usuarios que no existen aún en GO INTEGRO: {len(df_new_users)}")

    ## Agrega código de grupos dado desde GO INTEGRO
    df_new_users = update_users_with_group_items(df_new_users, df_group_items)

    ## Escribe los usuarios por importar en GO INTEGRO
    result = create_users_api_go_integro(df_new_users, token)

## 2. Toma los usuarios con cambios frente a GO INTEGRO
print("Busca cambios en los usuarios registrados")
df_users_to_update = compare_users(df_users, df_users_go)

if not df_users_to_update.empty:
    print(f"Usuarios por actualizar en GO INTEGRO: {len(df_users_to_update)}")

    ## Agrega código de grupos dado desde GO INTEGRO
    df_users_to_update = update_users_with_group_items(df_users_to_update, df_group_items)
    print(f"Usuario por patch ID: {df_users_to_update}")

    ## Actualiza los usuarios en GO Integro
    result_update = update_users_api_go_integro(df_users_to_update, token)

if(result != 'exitoso'):
    print(f"resultado de inserción: {result}")
    raise Exception(result)
if(result_update != 'exitoso'):
    print(f"resultado de actualización: {result_update}")
    raise Exception(result_update)