import pandas as pd

from utils.api_do_afiliacion import get_users_api_consulta_afiliacion
from utils.api_go_integro import get_users_api_go_integro, get_api_token, create_users_api_go_integro

## Lee los usuarios desde la API Afiliación de empresas
df_users = get_users_api_consulta_afiliacion('GO_INTEGRO')

if df_users is None or df_users.empty:
    print("No se encontraron usuarios en la API consulta afiliación empresa")
    exit()  

print("Se encontraron usuarios en la API consulta afiliación empresa")
print(df_users)

## Lee los usuarios ya registrados en GO INTEGRO
token = get_api_token()

df_users_go = get_users_api_go_integro(token)

print("Se encontraron usuarios en la API consulta afiliación empresa")
print(df_users_go)

## Filtra usuarios cuyo employee_id no existe en GO INTEGRO
df_new_users = df_users[~df_users["employee_id"].isin(df_users_go["employee_id"])]

print("Usuarios filtrados que no existen aún en GO INTEGRO (por employee_id)")
print(df_new_users)