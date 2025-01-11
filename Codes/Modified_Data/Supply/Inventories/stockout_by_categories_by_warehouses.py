import pandas as pd

# Load data
df_pt = pd.read_csv('/home/snparada/Spacionatural/Data/Historical/Supply/pt_inventories_by_day.csv')
df_me_mp = pd.read_csv('/home/snparada/Spacionatural/Data/Historical/Supply/me_mp_inventories_by_day.csv')
products = pd.read_csv('/home/snparada/Spacionatural/Data/Dim/Odoo/all_products.csv', usecols=['code', 'categ_id', 'all_product_tag_ids'])
shopify_products = pd.read_csv('/home/snparada/Spacionatural/Data/Dim/Shopify/products_shopify.csv', usecols=['status','variant_sku'])
active_shopify_products_list = shopify_products[shopify_products['status']=='active']['variant_sku'].to_list()

# Apply category extraction and tag extraction
products['category'] = products['categ_id'].apply(lambda x: eval(x)[1] if len(eval(x)) > 1 else None)
products['tag'] = products['all_product_tag_ids'].apply(lambda x: eval(x)[0] if len(eval(x)) > 0 else None)

# Merge the category into df_pt and df_me_mp based on the code and SKU respectively
df_pt = df_pt.merge(products[['code', 'category']], left_on='SKU', right_on='code', how='left').drop(columns=['code'])
df_me_mp = df_me_mp.merge(products[['code', 'category']], left_on='SKU', right_on='code', how='left').drop(columns=['code'])

# Define the warehouse columns for PT and ME-MP
pt_warehouses = ['FV/Stock', 'MELIF/Stock', 'FV/E-Commerce', 'FV/ML/Stock', 'JS/Stock']
me_mp_warehouses = ['FV/Materias Primas y Envases', 'JS/Materia Prima y Envases']

def add_missing_products(df, products, tag, warehouses):
    # Filtrar productos por la etiqueta (1 = PT, 2 = MP, 3 = ME)
    filtered_products = products[products['tag'] == tag]

    # Extraer todas las fechas únicas del dataframe
    unique_dates = df['date'].unique()

    # Encontrar los productos que faltan en el dataframe
    missing_products = filtered_products[~filtered_products['code'].isin(df['SKU'])]

    # Crear un dataframe vacío para almacenar productos faltantes
    missing_df = pd.DataFrame()

    # Generar filas para productos faltantes en todas las fechas
    for date in unique_dates:
        temp_df = pd.DataFrame({
            'SKU': missing_products['code'],
            'category': missing_products['category'],
            'date': date
        })
        
        # Agregar columnas de bodegas con valor 0
        for warehouse in warehouses:
            temp_df[warehouse] = 0
        
        # Concatenar con el dataframe de productos faltantes
        missing_df = pd.concat([missing_df, temp_df], ignore_index=True)
    missing_df = missing_df
    # Unir el dataframe original con los productos faltantes
    merged_df = pd.concat([df, missing_df], ignore_index=True)
    return merged_df



# Add missing PT products to df_pt
df_pt = add_missing_products(df_pt, products, 1, pt_warehouses)
# Add missing MP and ME products to df_me_mp
df_me_mp = add_missing_products(df_me_mp, products, 2, me_mp_warehouses)  # For MP
df_me_mp = add_missing_products(df_me_mp, products, 3, me_mp_warehouses)  # For ME

# Last filtering with just the shopify products that are actives
df_pt = df_pt[df_pt['SKU'].isin(active_shopify_products_list)]


# Function to calculate %stockout for a given dataframe, warehouse columns, and group by date, category, and warehouse
def calculate_stockout(df, warehouses):
    stockout_data = []
    
    for warehouse in warehouses:
        # Filter products with stock <= 0 for each warehouse
        df_warehouse = df[['date', 'category', warehouse]].copy()
        
        # Loop over each unique date and category
        for (date, category), group in df_warehouse.groupby(['date', 'category']):
            total_products = len(group)  # Total number of products in the category for the date
            
            # Filter for products in quiebre de stock (<= 0)
            stockout_df = group[group[warehouse] <= 0]
            stockout_percentage = (len(stockout_df) / total_products) * 100 if total_products > 0 else 0
            
            # Add the stockout percentage data
            stockout_data.append({
                '%stockout': stockout_percentage,
                'warehouse': warehouse,
                'date': date,
                'category': category
            })
            
        # Calculate global stockout percentage for all products for the warehouse
        for date, group in df_warehouse.groupby('date'):
            total_products_global = len(group)
            stockout_df_global = group[group[warehouse] <= 0]
            stockout_percentage_global = (len(stockout_df_global) / total_products_global) * 100 if total_products_global > 0 else 0
            
            # Add global stockout data
            stockout_data.append({
                '%stockout': stockout_percentage_global,
                'warehouse': warehouse,
                'date': date,
                'category': 'Global'
            })
    
    return pd.DataFrame(stockout_data)

# Calculate stockout for PT and ME-MP dataframes
pt_stockout_data = calculate_stockout(df_pt, pt_warehouses)
me_mp_stockout_data = calculate_stockout(df_me_mp, me_mp_warehouses)

# Save the results
pt_stockout_data.to_csv('/home/snparada/Spacionatural/Data/Historical/Supply/pt_stockout_categories_by_day.csv', index=False)
me_mp_stockout_data.to_csv('/home/snparada/Spacionatural/Data/Historical/Supply/me_mp_stockout_categories_by_day.csv',  index=False)