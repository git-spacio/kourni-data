import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
from database_lib.database import DB
from shopify_lib.products import ShopifyProducts
from LLM_lib.llm import LLM
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import time

# Clean HTML from body_html column
def clean_html(html_text):
    if pd.isna(html_text):
        return ""
    soup = BeautifulSoup(html_text, 'html.parser')
    return soup.get_text(separator=' ').strip()

shopify_product = ShopifyProducts()
db = DB()
llm = LLM()

all_products_in_shopify = shopify_product.read_all_products_in_dataframe()
all_products_in_shopify['body_html'] = all_products_in_shopify ['body_html'].apply(clean_html)

all_products_in_database = db.read_table_in_df('shopify_products')

if len(all_products_in_database) > 0:
    print("Iniciando proceso de comparación de productos...")
    # Create product_key for new data
    print("Generando product_keys para nuevos datos...")
    all_products_in_shopify['product_key'] = all_products_in_shopify['title'] + ' - ' + all_products_in_shopify['variant_title']
    all_products_in_shopify['product_key'] = all_products_in_shopify['product_key'].apply(lambda x: x.replace(' - nan', '').replace(' - None', ''))

    # Identify new variants
    print("Identificando nuevas variantes...")
    # Convertir variant_id a string en ambos DataFrames
    all_products_in_database['variant_id'] = all_products_in_database['variant_id'].astype(str)
    all_products_in_shopify['variant_id'] = all_products_in_shopify['variant_id'].astype(str)

    existing_variant_ids = set(all_products_in_database['variant_id'].tolist())
    new_variants_mask = ~all_products_in_shopify['variant_id'].isin(existing_variant_ids)
    new_variants_df = all_products_in_shopify[new_variants_mask].copy()
    print(f"Se encontraron {len(new_variants_df)} nuevas variantes")

    # Identify modified products
    print("Identificando productos modificados...")
    modified_mask = all_products_in_shopify['variant_id'].isin(existing_variant_ids)
    potentially_modified_df = all_products_in_shopify[modified_mask].copy()
    
    if len(potentially_modified_df) > 0:
        print(f"Comparando {len(potentially_modified_df)} productos existentes...")
        comparison_df = potentially_modified_df.merge(
            all_products_in_database[['variant_id', 'title', 'variant_title', 'product_key', 
                                        'status', 'variant_price', 'variant_inventory_quantity',
                                        'variant_compare_at_price', 'variant_sku', 'tags',
                                        'vendor', 'product_type', 'body_html']],
            on='variant_id',
            suffixes=('_new', '_old')
        )
        
        # Asegurar tipos consistentes antes de la comparación
        comparison_columns = ['title', 'variant_title', 'status', 'variant_price', 
                             'variant_inventory_quantity', 'variant_compare_at_price', 
                             'variant_sku', 'tags', 'vendor', 'product_type', 'body_html']

        for col in comparison_columns:
            # Convertir columnas _new a string
            comparison_df[f'{col}_new'] = comparison_df[f'{col}_new'].astype(str)
            # Convertir columnas _old a string
            comparison_df[f'{col}_old'] = comparison_df[f'{col}_old'].astype(str)
            # Reemplazar 'nan' y 'None' por cadena vacía para consistencia
            comparison_df[f'{col}_new'] = comparison_df[f'{col}_new'].replace({'nan': '', 'None': ''})
            comparison_df[f'{col}_old'] = comparison_df[f'{col}_old'].replace({'nan': '', 'None': ''})

        modified_mask = (comparison_df['title_new'] != comparison_df['title_old']) | \
                       (comparison_df['variant_title_new'] != comparison_df['variant_title_old']) | \
                       (comparison_df['status_new'] != comparison_df['status_old']) | \
                       (comparison_df['variant_price_new'] != comparison_df['variant_price_old']) | \
                       (comparison_df['variant_compare_at_price_new'] != comparison_df['variant_compare_at_price_old']) | \
                       (comparison_df['variant_sku_new'] != comparison_df['variant_sku_old']) | \
                       (comparison_df['tags_new'] != comparison_df['tags_old']) | \
                       (comparison_df['vendor_new'] != comparison_df['vendor_old']) | \
                       (comparison_df['product_type_new'] != comparison_df['product_type_old']) | \
                       (comparison_df['body_html_new'] != comparison_df['body_html_old'])
        modified_variants_df = potentially_modified_df[potentially_modified_df['variant_id'].isin(
            comparison_df[modified_mask]['variant_id']
        )].copy()
        print(f"Se encontraron {len(modified_variants_df)} productos modificados")
    else:
        modified_variants_df = pd.DataFrame()

    # Generate embeddings only for new and modified products
    products_needing_embeddings = pd.concat([new_variants_df, modified_variants_df])
    
    if len(products_needing_embeddings) > 0:
        print(f"\nGenerando embeddings para {len(products_needing_embeddings)} productos...")
        embeddings_dict = {}
        
        # Create embedding column if it doesn't exist
        if 'embedding' not in all_products_in_shopify.columns:
            all_products_in_shopify['embedding'] = None
        
        for idx, row in tqdm(products_needing_embeddings.iterrows(), 
                            desc="Generando embeddings", 
                            unit="producto"):
            embedding = llm.generate_embedding(row['product_key'])
            embeddings_dict[idx] = embedding
            time.sleep(0.5) 
        
        print("\nActualizando embeddings en el DataFrame principal...")
        for idx, embedding in embeddings_dict.items():
            all_products_in_shopify.at[idx, 'embedding'] = embedding

        if len(new_variants_df) > 0:
            print("\nGuardando nuevas variantes en la base de datos...")
            new_variants_with_embeddings = all_products_in_shopify[
                all_products_in_shopify['variant_id'].isin(new_variants_df['variant_id'])
            ]
            new_variants_with_embeddings.to_sql('shopify_products', db.engine, if_exists='append', index=False)
            print(f"✓ {len(new_variants_df)} nuevas variantes agregadas")

        if len(modified_variants_df) > 0:
            print("\nActualizando variantes modificadas en la base de datos...")
            modified_variants_with_embeddings = all_products_in_shopify[
                all_products_in_shopify['variant_id'].isin(modified_variants_df['variant_id'])
            ]
            for _, row in tqdm(modified_variants_with_embeddings.iterrows(), 
                             desc="Actualizando registros", 
                             total=len(modified_variants_with_embeddings)):
                db.update_by_direct_query(
                    'shopify_products',
                    """SET 
                        title = :title,
                        variant_title = :variant_title,
                        product_key = :product_key,
                        embedding = :embedding,
                        body_html = :body_html,
                        updated_at = :updated_at,
                        variant_price = :variant_price,
                        variant_inventory_quantity = :variant_inventory_quantity,
                        variant_compare_at_price = :variant_compare_at_price,
                        variant_sku = :variant_sku,
                        status = :status,
                        tags = :tags,
                        vendor = :vendor,
                        product_type = :product_type
                    WHERE variant_id = :variant_id""",
                    params=row.to_dict()
                )
            print(f"✓ {len(modified_variants_df)} variantes modificadas actualizadas")

        print("\nGuardando datos actualizados en CSV...")
        all_products_in_shopify.to_csv('/home/snparada/Spacionatural/Data/Dim/Shopify/products_shopify.csv', index=False)
        print("✓ CSV actualizado exitosamente")
    else:
        print("\n✓ No hay productos nuevos o modificados para procesar")

else:
    print("Base de datos vacía, iniciando creación inicial...")

    # Create product_key column
    all_products_in_shopify['product_key'] = all_products_in_shopify['title'] + ' - ' + all_products_in_shopify['variant_title']
    all_products_in_shopify['product_key'] = all_products_in_shopify['product_key'].apply(lambda x: x.replace(' - nan', '').replace(' - None', ''))

    # Generate embeddings for each product key
    print(f"\nGenerando embeddings para {len(all_products_in_shopify)} productos...")
    
    embeddings = []
    for product_key in tqdm(all_products_in_shopify['product_key'], 
                          desc="Generando embeddings", 
                          unit="producto"):
        embedding = llm.generate_embedding(product_key)
        embeddings.append(embedding)
        time.sleep(0.5)
    
    print("\nAgregando embeddings al DataFrame...")
    all_products_in_shopify['embedding'] = embeddings

    print("\nCreando tabla en la base de datos...")
    columns = [
        {'name': 'variant_id', 'type': 'Text'},
        {'name': 'id', 'type': 'Text'},
        {'name': 'product_key', 'type': 'Text'},
        {'name': 'embedding', 'type': 'vector(1536)'},
        {'name': 'title', 'type': 'Text'},
        {'name': 'vendor', 'type': 'Text'},
        {'name': 'body_html', 'type': 'Text'},
        {'name': 'product_type', 'type': 'Text'},
        {'name': 'created_at', 'type': 'Text'},
        {'name': 'handle', 'type': 'Text'},
        {'name': 'updated_at', 'type': 'Text'},
        {'name': 'published_at', 'type': 'Text'},
        {'name': 'tags', 'type': 'Text'},
        {'name': 'status', 'type': 'Text'},
        {'name': 'variant_title', 'type': 'Text'},
        {'name': 'variant_compare_at_price', 'type': 'Text'},
        {'name': 'variant_price', 'type': 'Text'},
        {'name': 'variant_sku', 'type': 'Text'},
        {'name': 'variant_inventory_quantity', 'type': 'Integer'}
    ]

    db.create_new_table('shopify_products', columns)
    
    print("\nGuardando productos en la base de datos...")
    columns_to_convert = ['id', 'variant_id']
    for col in columns_to_convert:
        if col in all_products_in_shopify.columns:
            all_products_in_shopify[col] = all_products_in_shopify[col].astype(str)

    all_products_in_shopify.to_sql('shopify_products', db.engine, if_exists='append', index=False)
    
    print("\nGuardando datos en CSV...")
    all_products_in_shopify.to_csv('/home/snparada/Spacionatural/Data/Dim/Shopify/products_shopify.csv', index=False)
    
    print("✓ Proceso inicial completado exitosamente")
