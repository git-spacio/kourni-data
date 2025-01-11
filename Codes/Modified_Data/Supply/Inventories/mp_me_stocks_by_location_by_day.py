import pandas as pd
from datetime import datetime
import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
from sheets_lib.main_sheets import GoogleSheets

# Read the dataframe from the given Google Sheets ID
sheet_id = '1qXFVi8WoyBnfxmsrUZ6BmB5gvIHU8oocbSVUkGEtcGY'
# Initialize GoogleSheets object
gs = GoogleSheets(sheet_id)



try:
    # Intentar leer el DataFrame y mostrar las columnas
    df = gs.read_dataframe('ME-MP')

except Exception as e:
    print("\nError al leer los datos:")
    print(f"Tipo de error: {type(e).__name__}")
    print(f"Mensaje de error: {str(e)}")
    sys.exit(1)

# Delete the columns 'Nombre', 'Cotizaciones', 'E-Commerce', 'Mercado Libre', 'Tienda Sabaj' from the dataframe
columns_to_delete = ['Nombre', 'tags']
df.drop(columns=[col for col in columns_to_delete if col in df.columns], inplace=True)

# Add a column with the current date
df['date'] = datetime.now().strftime('%Y-%m-%d')

# Define the file path to save the updated dataframe
file_path = '/home/snparada/Spacionatural/Data/Historical/Supply/me_mp_inventories_by_day.csv'
df.to_csv(file_path, index=False)

# Read the existing data from the CSV file
try:
    df_existing = pd.read_csv(file_path, low_memory=False)
    # Append the new data to the existing data
    df_updated = pd.concat([df_existing, df])
except FileNotFoundError:
    # If the file does not exist, use the new data as the updated data
    df_updated = df

# Remove duplicate rows from the dataframe
df_updated.drop_duplicates(inplace=True)

# Save the updated dataframe to the CSV file
df_updated.to_csv(file_path, index=False)
