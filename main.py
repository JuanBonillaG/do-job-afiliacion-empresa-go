from utils.api_do_afiliacion import get_users_go_integro
#from google.cloud import storage 
#from datetime import datetime, timedelta

## Lee los usuarios de go integro desde la API
df_users = get_users_go_integro()

if df_users.empty:
    print("No se encontraron usuarios en la API de Go Integro")
    exit()  

print("Se encontraron usuarios en la API de Go Integro")
print(df_users)