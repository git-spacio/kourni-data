import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
from database_lib.database import DB
from odoo_lib.product import OdooProduct
import pandas as pd

db = DB()
shopify_products = db.read_table_in_df('shopify_products')

# Uso de la clase para extraer el inventario
odoo_product = OdooProduct()

# Extraer y renombrar las columnas relevantes de Odoo
relevant_columns = ['id', 'product_tmpl_id', 'is_product_variant', 'name', 'default_code', 'code', 'barcode']
all_products = odoo_product.read_all_products_in_dataframe()
all_products = all_products[relevant_columns]

# Asegurarse de que product_tmpl_id sea solo el ID numérico
if isinstance(all_products['product_tmpl_id'].iloc[0], (list, tuple)):
    all_products['product_tmpl_id'] = all_products['product_tmpl_id'].apply(lambda x: x[0] if isinstance(x, (list, tuple)) else x)

# Renombrar columnas de Odoo para evitar conflictos
odoo_columns_rename = {
    'product_tmpl_id': 'odoo_parent_id',
    'name': 'odoo_name',
    'id': 'odoo_id',
    'default_code': 'odoo_default_code',
    'code': 'odoo_code',
    'barcode': 'odoo_barcode',
    'is_product_variant': 'odoo_is_variant'
}
all_products = all_products.rename(columns=odoo_columns_rename)

print(all_products.head())
print(shopify_products.head())

# Limpiar y convertir los códigos a string antes del merge
shopify_products['variant_sku'] = shopify_products['variant_sku'].astype(str).str.strip()
all_products['odoo_code'] = all_products['odoo_code'].astype(str).str.strip()

# Solo intentar eliminar las columnas si existen
columns_to_drop = ['odoo_code', 'odoo_default_code', 'odoo_name', 'odoo_barcode', 
                  'odoo_is_variant', 'odoo_parent_id', 'odoo_id']
existing_columns = [col for col in columns_to_drop if col in shopify_products.columns]
if existing_columns:
    shopify_products = shopify_products.drop(columns=existing_columns)

# Hacer el merge entre Shopify y Odoo usando variant_sku y default_code
merged_products = shopify_products.merge(
    all_products,
    left_on='variant_sku',
    right_on='odoo_code',
    how='left',
    suffixes=('', '_y')
)

# Imprimir algunos ejemplos para debug
print("\nEjemplos de SKUs después de la limpieza:")
print("Shopify SKUs:", shopify_products['variant_sku'].head().tolist())
print("Odoo SKUs:", all_products['odoo_default_code'].head().tolist())

# Limpiar columnas duplicadas con sufijo _y
columns_to_drop = [col for col in merged_products.columns if col.endswith('_y')]
merged_products = merged_products.drop(columns=columns_to_drop)

print("\nEjemplo de valores en variant_sku:", shopify_products['variant_sku'].head())
print("Ejemplo de valores en odoo_default_code:", all_products['odoo_default_code'].head())

# Corregir el query de UPDATE - eliminar la duplicación
update_query = """
SET 
    odoo_id = :odoo_id,
    odoo_parent_id = :odoo_parent_id,
    odoo_name = :odoo_name,
    odoo_default_code = :odoo_default_code,
    odoo_code = :odoo_code,
    odoo_barcode = :odoo_barcode,
    odoo_is_variant = :odoo_is_variant
WHERE variant_sku = :variant_sku
"""

# Mostrar información del merge
print("\nCantidad de productos en Shopify:", len(shopify_products))
print("Cantidad de productos en Odoo:", len(all_products))
print("Cantidad de productos después del merge:", len(merged_products))
print("\nPrimeros registros del merge:")
print(merged_products[['variant_sku', 'odoo_id', 'odoo_name', 'odoo_default_code']].head())

# Verificar productos sin match
print("\nCantidad de productos de Shopify sin match en Odoo:", 
      merged_products['odoo_id'].isna().sum())


# Definir las nuevas columnas de Odoo que queremos añadir a shopify_products
new_columns = [
    {'name': 'odoo_id', 'type': 'Integer'},
    {'name': 'odoo_parent_id', 'type': 'Integer'},
    {'name': 'odoo_name', 'type': 'Text'},
    {'name': 'odoo_default_code', 'type': 'Text'},
    {'name': 'odoo_code', 'type': 'Text'},
    {'name': 'odoo_barcode', 'type': 'Text'},
    {'name': 'odoo_is_variant', 'type': 'Boolean'}
]

# Añadir las nuevas columnas a la tabla shopify_products
db.create_new_columns('shopify_products', new_columns)

print("\n=== Iniciando actualización de primeros 10 productos ===")

# Actualizar solo las primeras 10 filas del merge en la base de datos
for idx, row in merged_products.iterrows():
    params = {
        'odoo_id': None if pd.isna(row.get('odoo_id')) else row['odoo_id'],
        'odoo_parent_id': None if pd.isna(row.get('odoo_parent_id')) else row['odoo_parent_id'],
        'odoo_name': None if pd.isna(row.get('odoo_name')) else row['odoo_name'],
        'odoo_default_code': None if pd.isna(row.get('odoo_default_code')) else row['odoo_default_code'],
        'odoo_code': None if pd.isna(row.get('odoo_code')) else row['odoo_code'],
        'odoo_barcode': None if pd.isna(row.get('odoo_barcode')) else row['odoo_barcode'],
        'odoo_partner_ref': None if pd.isna(row.get('odoo_partner_ref')) else row['odoo_partner_ref'],
        'odoo_is_variant': None if pd.isna(row.get('odoo_is_variant')) else bool(row['odoo_is_variant']),
        'variant_sku': row['variant_sku']
    }
    
    # Imprimir los valores que se van a actualizar
    for key, value in params.items():
        print(f"{key}: {value}")
    
    try:
        db.update_by_direct_query('shopify_products', update_query, params)
        print("✓ Actualización exitosa")
    except Exception as e:
        print(f"✗ Error en la actualización: {str(e)}")

print("\n=== Verificando datos actualizados ===")
# Verificar los datos actualizados
verification_results = db.read_table_in_df('shopify_products')
# Mostrar todos los valores no nulos en variant_sku y odoo_id para debug
print("\nMuestra de SKUs y sus IDs de Odoo:")
print(verification_results[['variant_sku', 'odoo_id']].head(10))

# Filtrar y mostrar los resultados finales
verification_results = verification_results[verification_results['odoo_id'].notnull()]
verification_results = verification_results[['variant_sku', 'odoo_id', 'odoo_parent_id', 'odoo_name']].head(10)
print("\nDatos en la base de datos después de la actualización:")
print(verification_results)

print("\nProceso de prueba completado")