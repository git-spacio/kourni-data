import sys
sys.path.append('/home/snparada/Spacionatural/Libraries')
from shopify_lib.blogs import ShopifyBlogs
import pandas as pd
from bs4 import BeautifulSoup
import re
from database_lib.database import DB
from LLM_lib.llm import LLM
from tqdm import tqdm
import time

def clean_html(html_content):
    """
    Limpia el contenido HTML usando BeautifulSoup
    """
    if pd.isna(html_content):
        return ''
    
    # Crear objeto BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Eliminar scripts y estilos
    for script in soup(["script", "style"]):
        script.decompose()
    
    # Obtener texto
    text = soup.get_text(separator=' ')
    
    # Limpiar espacios extra y líneas nuevas
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def get_existing_posts():
    """
    Obtiene los posts existentes de la base de datos incluyendo el embedding
    """
    try:
        # Usar el método read_table_in_df de la clase DB
        df = db.read_table_in_df('shopify_post_blogs', ['id', 'updated_at', 'title_embedding'])
        return df.set_index('id').to_dict('index')
    except:
        return {}

def create_blogs_table():
    query = """
    CREATE TABLE IF NOT EXISTS shopify_post_blogs (
        id BIGINT PRIMARY KEY,
        title TEXT,
        body_html TEXT,
        body_text TEXT,
        author TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        published_at TIMESTAMP,
        tags TEXT,
        handle TEXT,
        title_embedding VECTOR(1536)
    );
    """
    try:
        db.execute_query(query)
        print("Table shopify_post_blogs created successfully with the updated structure")
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        raise


shopify_blogs = ShopifyBlogs()
db = DB()

# Obtener la lista de blogs disponibles
blogs = shopify_blogs.read_all_blogs()

if not blogs:
    print("No se encontraron blogs")
    exit()

all_posts_df = pd.DataFrame()


for blog in blogs:  
    # Obtener posts del blog actual
    df_posts = shopify_blogs.read_all_blog_posts_df(str(blog['id']))
    
    if not df_posts.empty:
        # Añadir información del blog
        df_posts['blog_title'] = blog['title']
        df_posts['blog_handle'] = blog['handle']
        
        # Limpiar contenido HTML
        df_posts['body_text'] = df_posts['body_html'].apply(clean_html)
        df_posts['summary_text'] = df_posts['summary_html'].apply(clean_html)
        
        # Concatenar con el DataFrame principal
        all_posts_df = pd.concat([all_posts_df, df_posts], ignore_index=True)


if not all_posts_df.empty:
    # Obtener posts existentes
    existing_posts = db.read_table_in_df('shopify_post_blogs')
    
    if len(existing_posts) > 0:
        print("Iniciando proceso de comparación de posts...")
        
        # Al cargar los datos
        existing_posts['id'] = existing_posts['id'].astype(str)
        all_posts_df['id'] = all_posts_df['id'].astype(str)
        
        # Luego la comparación será más limpia
        existing_ids = set(existing_posts['id'].tolist())
        new_posts_mask = ~all_posts_df['id'].isin(existing_ids)
        new_posts_df = all_posts_df[new_posts_mask].copy()
        print(f"Se encontraron {len(new_posts_df)} posts nuevos")
        
        # Identificar posts modificados
        modified_mask = all_posts_df['id'].astype(str).isin(existing_ids)
        potentially_modified_df = all_posts_df[modified_mask].copy()
        
        if len(potentially_modified_df) > 0:
            print(f"Comparando {len(potentially_modified_df)} posts existentes...")
            comparison_df = potentially_modified_df.merge(
                existing_posts[['id', 'title', 'body_html', 'updated_at']],
                on='id',
                suffixes=('_new', '_old')
            )
            
            # Asegurar tipos consistentes
            for col in ['title', 'body_html', 'updated_at']:
                comparison_df[f'{col}_new'] = comparison_df[f'{col}_new'].astype(str)
                comparison_df[f'{col}_old'] = comparison_df[f'{col}_old'].astype(str)
                comparison_df[f'{col}_new'] = comparison_df[f'{col}_new'].replace({'nan': '', 'None': ''})
                comparison_df[f'{col}_old'] = comparison_df[f'{col}_old'].replace({'nan': '', 'None': ''})
            
            modified_mask = (comparison_df['title_new'] != comparison_df['title_old']) | \
                          (comparison_df['body_html_new'] != comparison_df['body_html_old']) | \
                          (comparison_df['updated_at_new'] != comparison_df['updated_at_old'])
            
            modified_posts_df = potentially_modified_df[potentially_modified_df['id'].isin(
                comparison_df[modified_mask]['id']
            )].copy()
            print(f"Se encontraron {len(modified_posts_df)} posts modificados")
        else:
            modified_posts_df = pd.DataFrame()
        
        # Generar embeddings solo para posts nuevos y modificados
        posts_needing_embeddings = pd.concat([new_posts_df, modified_posts_df])
        
        if len(posts_needing_embeddings) > 0:
            print(f"\nGenerando embeddings para {len(posts_needing_embeddings)} posts...")
            llm = LLM()
            embeddings_dict = {}
            
            if 'title_embedding' not in all_posts_df.columns:
                all_posts_df['title_embedding'] = None
            
            for idx, row in tqdm(posts_needing_embeddings.iterrows(), 
                                desc="Generando embeddings", 
                                unit="post"):
                embedding = llm.generate_embedding(row['title'])
                embeddings_dict[idx] = embedding
                time.sleep(0.5)
            
            print("\nActualizando embeddings en el DataFrame principal...")
            for idx, embedding in embeddings_dict.items():
                all_posts_df.at[idx, 'title_embedding'] = embedding
            
            if len(new_posts_df) > 0:
                print("\nGuardando nuevos posts en la base de datos...")
                new_posts_with_embeddings = all_posts_df[
                    all_posts_df['id'].isin(new_posts_df['id'])
                ]
                new_posts_with_embeddings.to_sql('shopify_post_blogs', db.engine, if_exists='append', index=False)
                print(f"✓ {len(new_posts_df)} nuevos posts agregados")
            
            if len(modified_posts_df) > 0:
                print("\nActualizando posts modificados en la base de datos...")
                modified_posts_with_embeddings = all_posts_df[
                    all_posts_df['id'].isin(modified_posts_df['id'])
                ]
                for _, row in tqdm(modified_posts_with_embeddings.iterrows(), 
                                desc="Actualizando registros", 
                                total=len(modified_posts_with_embeddings)):
                    db.update_by_direct_query(
                        'shopify_post_blogs',
                        """SET 
                            title = :title,
                            author = :author,
                            body_html = :body_html,
                            body_text = :body_text,
                            summary_html = :summary_html,
                            summary_text = :summary_text,
                            handle = :handle,
                            blog_title = :blog_title,
                            blog_handle = :blog_handle,
                            updated_at = :updated_at,
                            published_at = :published_at,
                            title_embedding = :title_embedding
                        WHERE id = :id""",
                        params=row.to_dict()
                    )
                print(f"✓ {len(modified_posts_df)} posts modificados actualizados")
        else:
            print("\n✓ No hay posts nuevos o modificados para procesar")
    
    else:
        # Caso inicial: base de datos vacía
        print("Base de datos vacía, iniciando creación inicial...")
        create_blogs_table()
        
        print(f"\nGenerando embeddings para {len(all_posts_df)} posts...")
        llm = LLM()
        
        embeddings = []
        for title in tqdm(all_posts_df['title'], 
                        desc="Generando embeddings", 
                        unit="post"):
            embedding = llm.generate_embedding(title)
            embeddings.append(embedding)
            time.sleep(0.5)
        
        all_posts_df['title_embedding'] = embeddings
        
        print("\nGuardando posts en la base de datos...")
        all_posts_df.to_sql('shopify_post_blogs', db.engine, if_exists='append', index=False)
        print("✓ Proceso inicial completado exitosamente")
