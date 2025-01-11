import ast
import pandas as pd
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/klaviyo_lib')
from profiles import KlaviyoProfiles

klaviyo_profiles_obj = KlaviyoProfiles()

klaviyo_customers = klaviyo_profiles_obj.read_all_profiles()

def extract_email(row):
    # Asegurar que se maneja como un diccionario
    try:
        # Verificar si es una cadena para evaluar
        if isinstance(row, str):
            attributes = ast.literal_eval(row)
        else:
            attributes = row  # Si ya es un diccionario, se usa directamente

        # Retornar el email del diccionario
        return attributes.get('email', '')

    except Exception as e:
        print(f"Error processing row: {row} with error: {e}")
        return ''


klaviyo_customers['email'] = klaviyo_customers['attributes'].apply(extract_email)

klaviyo_customers = klaviyo_customers[['email','type','id','attributes','relationships','links']]

klaviyo_customers.to_csv('/home/snparada/Spacionatural/Data/Dim/customers_klaviyo.csv', index = False)