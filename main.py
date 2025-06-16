import pandas as pd

from utils.api_do_afiliacion import get_users_api_consulta_afiliacion
from utils.api_go_integro import get_users_api_go_integro, get_api_token, get_group_items_api_go_integro,create_users_api_go_integro, update_users_api_go_integro
from utils.job_functions import compare_users, update_users_with_group_items

## Lee los usuarios desde la API Afiliación de empresas
df_users = get_users_api_consulta_afiliacion('GO_INTEGRO')


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

if df_users is None or df_users.empty:
    print("No se encontraron usuarios en la API consulta afiliación empresa")
    exit()  

print(f"Cantidad de usuarios encontrados en la API consulta afiliación empresa: {len(df_users)}")

## Obtiene token de acceso GO INTEGRO
token = get_api_token()

## Lee los usuarios ya registrados en GO INTEGRO
df_users_go = get_users_api_go_integro(token)
## Lee los grupos creados en GO INTEGRO
df_group_items = get_group_items_api_go_integro(token)

# Asegura la consistencia de tipos
df_users["document"] = df_users["document"].astype(str)
df_users_go["document"] = df_users_go["document"].astype(str)

## Filtra usuarios cuyo documento no existe en GO INTEGRO
df_new_users = df_users[~df_users["document"].isin(df_users_go["document"])]

result        = 'exitoso'
result_update = 'exitoso'
if not df_new_users.empty:
    print(f"Usuarios que no existen aún en GO INTEGRO: {len(df_new_users)}")
    print(f"Usuario por crear group_item_id: {df_new_users[0]['group_item_id']}")

    ## Cruza los usuarios por insertar con grupos de GO INTEGRO
    df_new_users = update_users_with_group_items(df_new_users, df_group_items)

    ## Escribe los usuarios por importar en GO INTEGRO
    result = create_users_api_go_integro(df_new_users, token)

## Toma los usuarios con cambios frente a GO INTEGRO
print("Busca cambios en los usuarios registrados")
df_users_to_update = compare_users(df_users, df_users_go)

if not df_users_to_update.empty:
    print(f"Usuarios por actualizar en GO INTEGRO: {len(df_users_to_update)}")
    print(f"Usuario por patch ID: {df_users_to_update[0]['id']}")

    ## Cruza los usuarios por insertar con grupos de GO INTEGRO
    df_users_to_update = update_users_with_group_items(df_users_to_update, df_group_items)

    ## Actualiza los usuarios en GO Integro
    result_update = update_users_api_go_integro

if(result != 'exitoso'):
    raise Exception(result)
if(result_update != 'exitoso'):
    print(f"result_update {result_update}")
    raise Exception(result_update)