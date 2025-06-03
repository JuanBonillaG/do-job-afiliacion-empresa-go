import pandas as pd

from utils.api_do_afiliacion import get_users_api_consulta_afiliacion
from utils.api_go_integro import get_users_api_go_integro, get_api_token, create_users_api_go_integro

## Lee los usuarios desde la API Afiliación de empresas
#df_users = get_users_api_consulta_afiliacion('GO_INTEGRO')

df_users = pd.DataFrame([{
    "first_name": "Antiguo Usuario",
    "email": None,
    "last_name": "manual",
    "employee_id": 123456,
    "document_type": "CC",
    "groups": "Empresas A&A:COLOMBIA SAS",
    "document": 123456,
    "external_id": "SB-987654321"
},
{
    "first_name": "Nuevo Usuario",
    "email": None,
    "last_name": "job",
    "employee_id": 987654321,
    "document_type": "CC",
    "groups": "Empresas A&A:COLOMBIA SAS",
    "document": 987654321,
    "external_id": "SB-4389278"
}])

if df_users is None or df_users.empty:
    print("No se encontraron usuarios en la API consulta afiliación empresa")
    exit()  

print("Se encontraron usuarios en la API consulta afiliación empresa")
#print(df_users)

## Lee los usuarios ya registrados en GO INTEGRO
token = get_api_token()

df_users_go = get_users_api_go_integro(token)

print("Se encontraron usuarios en la API GO Integro")
print(df_users_go["employee_id"])

## Asegura la consistencia de tipo de employee_id
df_users["employee_id"] = df_users["employee_id"].astype(str)
df_users_go["employee_id"] = df_users_go["employee_id"].astype(str)
df_users["document"] = df_users["document"].astype(str)

## Filtra usuarios cuyo employee_id no existe en GO INTEGRO
df_new_users = df_users[~df_users["employee_id"].isin(df_users_go["employee_id"])]

print("Usuarios filtrados que no existen aún en GO INTEGRO")
print(df_new_users["employee_id"])

## Escribe los usuarios por importar en GO INTEGRO
result = create_users_api_go_integro(df_new_users,token)

if(result != 'exitoso'):
    raise Exception(result) 