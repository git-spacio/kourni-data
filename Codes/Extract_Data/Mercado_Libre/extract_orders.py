import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/')
from mercado_libre_lib.orders import MeliOrders
from tqdm import tqdm
import pandas as pd
import time

orders = MeliOrders()

all_orders = orders.read_all_orders()

# Función para obtener información de envío
def get_shipping_info(shipping_id):
    try:
        time.sleep(0.1)
        return orders.read_shipping_by_id(shipping_id)
    except Exception as e:
        print(f"Error al obtener información de envío para ID {shipping_id}: {e}")
        return None

# Aplicar la función a la columna 'shipping' y expandir la información
tqdm.pandas(desc="Obteniendo información de envíos")
all_orders['shipping_info'] = all_orders['shipping'].progress_apply(lambda x: get_shipping_info(x['id']) if isinstance(x, dict) and 'id' in x else None)

# Expandir la columna 'shipping_info' en nuevas columnas
shipping_df = pd.json_normalize(all_orders['shipping_info'])
shipping_df.columns = ['shipping_' + col for col in shipping_df.columns]

# Combinar el DataFrame original con la información de envío expandida
all_orders = pd.concat([all_orders, shipping_df], axis=1)

# Eliminar las columnas originales 'shipping' y 'shipping_info' si lo deseas
all_orders = all_orders.drop(['shipping', 'shipping_info'], axis=1)

all_orders.to_csv('/home/snparada/Spacionatural/Data/Historical/Mercado_Libre/all_orders.csv', index=False)
