import pandas as pd

# Leer el archivo CSV
df = pd.read_csv('/home/snparada/Spacionatural/Data/Historical/historic_orders_laudus_with_items.csv', low_memory=False)

# Convertir la columna 'issuedDate' a datetime manejando múltiples formatos
df['issuedDate'] = pd.to_datetime(df['issuedDate'], format='ISO8601', errors='coerce')

# Verifica si alguna fila tiene fechas no convertidas correctamente
if df['issuedDate'].isnull().sum() > 0:
    print("Algunas fechas no pudieron ser convertidas, revisa los datos en esas filas.")
    # Opcional: Imprimir las filas con fechas no convertidas correctamente
    print(df[df['issuedDate'].isnull()])

# Continuar con la lógica anterior para agregar columnas de año, semana y mes

# Agregar las columnas de año y semana
df['year'] = df['issuedDate'].dt.year
df['week'] = df['issuedDate'].dt.isocalendar().week

# Crear el DataFrame por semanas
df_weekly = df.groupby(['items_product_sku', 'year', 'week'])['items_quantity'].sum().unstack(fill_value=0).reset_index()

# Guardar el DataFrame de semanas en un archivo CSV
df_weekly.to_csv('/home/snparada/Spacionatural/Data/Historical/weekly_sales_by_sku.csv', index=False)

# Agregar la columna de mes
df['month'] = df['issuedDate'].dt.month

# Crear el DataFrame por meses
df_monthly = df.groupby(['items_product_sku', 'year', 'month'])['items_quantity'].sum().unstack(fill_value=0).reset_index()

# Guardar el DataFrame de meses en un archivo CSV
df_monthly.to_csv('/home/snparada/Spacionatural/Data/Historical/monthly_sales_by_sku.csv', index=False)