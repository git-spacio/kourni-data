import sys
sys.path.append('/home/snparada/Spacionatural/Libraries/laudus_lib')
from orders import LaudusOrders
import pandas as pd

laudus_orders = LaudusOrders()


############################
## PART 1 : Historic Data ##
############################

non_updated_sales_file_path = '/home/snparada/Spacionatural/Data/Historical/historic_orders_laudus_with_items.csv'

# Ask user if they want to import all historical data
user_input = input("¿Quieres traer todos los datos históricos de nuevo? (y/n): ").strip().lower()
import_all_data = user_input

if import_all_data == 'y':
    df = laudus_orders.read_all_orders_with_items()
    df.drop_duplicates(inplace=True)
    df.to_csv(non_updated_sales_file_path, index=False)


##########################################
## PART 2 : Adding and loading new data ##
##########################################

sales = laudus_orders.read_lastest_orders(non_updated_sales_file_path) #Agregamos las nuevas ordenes al consolidado

##############################
## PART 3 : Data Preprocess ##
##############################

updated_df = laudus_orders.adding_sales_channel(sales)
updated_df.to_csv('/home/snparada/Spacionatural/Data/Historical/historic_orders_laudus_with_items.csv', index=False)
