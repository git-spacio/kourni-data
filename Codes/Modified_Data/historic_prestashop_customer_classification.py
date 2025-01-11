import pandas as pd
from datetime import datetime, timedelta
import re, json
from unidecode import unidecode

def load_data():
    order_detailed_df = pd.read_csv('/home/snparada/Spacionatural/Data/Historical/historic_carts_prestashop.csv', usecols=['id_order', 'product_name', 'category_name'], low_memory=False)
    orders_df = pd.read_csv('/home/snparada/Spacionatural/Data/Historical/historic_orders_prestashop_without_items.csv', usecols=['id_order', 'id_customer', 'date_add', 'valid', 'total_products'], low_memory=False)
    customers_df = pd.read_csv('/home/snparada/Spacionatural/Data/Dim/customers_prestashop.csv', usecols=['id_customer', 'firstname', 'lastname', 'email'], low_memory=False)
    return order_detailed_df, orders_df, customers_df

def prepare_data(orders_df, customers_df, order_detailed_df):
    orders_df = orders_df[orders_df['valid'] == 1]
    orders_df['date_add'] = pd.to_datetime(orders_df['date_add'])
    combined_df = pd.merge(orders_df, customers_df, on='id_customer')
    combined_df = pd.merge(combined_df, order_detailed_df, on='id_order')
    combined_df.sort_values(['id_customer', 'date_add'], inplace=True)

    # Asegurarse de filtrar correos no deseados antes de los cálculos
    combined_df = combined_df[combined_df['email'] != 'ventas.spacionatural@gmail.com']

    # Calcular 'days_between_orders'
    combined_df['previous_order_date'] = combined_df.groupby('id_customer')['date_add'].shift(1)
    combined_df['days_between_orders'] = (combined_df['date_add'] - combined_df['previous_order_date']).dt.days

    # Calcular 'last_purchase'
    combined_df['last_purchase'] = combined_df.groupby('id_customer')['date_add'].transform('max').apply(
        lambda x: '' + ('3 meses' if x >= datetime.now() - timedelta(days=90) else 
                                               '6 meses' if x >= datetime.now() - timedelta(days=180) else
                                               '12 meses' if x >= datetime.now() - timedelta(days=365) else
                                               '18 meses' if x >= datetime.now() - timedelta(days=365 * 1.5) else
                                               '24 meses' if x >= datetime.now() - timedelta(days=365 * 2) else
                                               '36 meses' if x >= datetime.now() - timedelta(days=365 * 3) else
                                               '48 meses' if x >= datetime.now() - timedelta(days=365 * 4) else
                                               'más de 4 años'))

    return combined_df

def classify_purchase(name, category_name):
    labels = set()
    name = unidecode(name.lower())
    category_mapping = {
        'TENSIOACTIVOS': 'Shampoo',
        'JABÓN SÓLIDO': 'Jabones',
        'VELA': 'Velas',
        'ENVASES PARA VELAS': 'Velas',
        'ESENCIAS PARA VELAS' : 'Velas',
        'ACEITES NATURALES': 'Aceites',
        'ACEITES ESENCIALES': 'Aromaterapia',
        'CREMAS':'Cremas',
        'EXFOLIANTES': 'Exfoliantes',
        'AGUA PARA ROPA': 'Productos Terminados',
        'BOMBAS EFERVECENTES' : 'Productos Terminados',
        'SALES DE BAÑO':'Productos Terminados',
        'JABONES DE LUFFA' :'Productos Terminados',
        'JABONES' :'Productos Terminados',
        'SABORES AROMÁTICOS' : 'Labiales',
        'MOLDES PARA VELAS': 'Velas',
        'LIPS': 'Productos Terminados',
    }

    for category, label in category_mapping.items():
        if category in category_name:
            labels.add(label)

    if 'prensa' in name:
        labels.add('Shampoo')

    if re.search(r'base\s*(de\s*)?jab[oó]n', name):
        labels.add('Jabones')

    if re.search(r'nova', name):
        labels.add('Cremas')
    if re.search(r'no ionica', name):
        labels.add('Cremas')

    if re.search(r'spray', name):
        labels.add('Home Spray')
    if re.search(r'atomizador', name):
        labels.add('Home Spray')

    if re.search(r'soya', name):
        labels.add('Velas')

    if re.search(r'acido citrico', name) or re.search(r'bicarbonato', name):
        labels.add('Bombas Efervecentes')
    if re.search(r'aceite esencial', name):
        labels.add('Aromaterapia')
    if re.search(r'sal mineral', name) or re.search(r'sal epsom', name):
        labels.add('Sales')

    return list(labels)

def calculate_avg_ticket(combined_df):
    # Agrupar por id_order y sumar total_products para cada pedido
    order_sums = combined_df.groupby('id_order').agg({'total_products': 'first'}).reset_index()

    # Agregar id_customer a order_sums para poder agrupar por cliente
    order_sums = pd.merge(order_sums, combined_df[['id_order', 'id_customer']], on='id_order', how='left')

    # Calcular el ticket promedio por cliente
    customer_tickets = order_sums.groupby('id_customer').agg({'total_products': 'mean'}).rename(columns={'total_products': 'Avg_Ticket'})

    return customer_tickets

def calculate_customer_metrics(combined_df):
    combined_df['labels'] = combined_df.apply(lambda row: classify_purchase(row['product_name'], row['category_name']), axis=1)

    avg_tickets = calculate_avg_ticket(combined_df)

    customers_summary = combined_df.groupby('id_customer').agg(
        Avg_Days_Between_Orders=('days_between_orders', 'mean'),
        Min_Days_Between_Orders=('days_between_orders', 'min'),
        Max_Days_Between_Orders=('days_between_orders', 'max'),
        Last_Purchase=('last_purchase', 'first'),
        Num_Orders=('id_order', 'nunique'),
        Labels=('labels', lambda x: json.dumps(list(set(sum(x, []))))),  # Serializa la lista de etiquetas a una cadena JSON
        firstname=('firstname', 'first'),
        lastname=('lastname', 'first'),
        email=('email', 'first')
    ).reset_index()

    customers_summary = pd.merge(customers_summary, avg_tickets, on='id_customer')
    customers_summary['monetary'] = customers_summary['Avg_Ticket'] * customers_summary['Num_Orders']
    
    return customers_summary

def classify_monetary_by_pareto(customers_summary):
    total_revenue = customers_summary['monetary'].sum()
    twenty_percent_revenue = total_revenue * 0.2  # Calcula el 20% de los ingresos totales.
    customers_summary = customers_summary.sort_values(by='monetary', ascending=False)
    customers_summary['cumulative_monetary'] = customers_summary['monetary'].cumsum()
    cutoff_index = customers_summary[customers_summary['cumulative_monetary'] <= twenty_percent_revenue].last_valid_index()
    cutoff_revenue = customers_summary.loc[cutoff_index, 'cumulative_monetary'] if cutoff_index is not None else 0
    customers_summary['monetary_classification'] = customers_summary['cumulative_monetary'].apply(lambda x: 'high_monetary' if x <= cutoff_revenue else 'low_monetary')

    # Imprimir detalles del cliente en el corte de Pareto
    pareto_cutoff_customer = customers_summary.loc[cutoff_index] if cutoff_index is not None else "No customer meets the 20% cutoff"
    print("\nPareto Cutoff Customer Details:")
    print(pareto_cutoff_customer)

    return customers_summary

def classify_avg_ticket(customers_summary):
    median_ticket = customers_summary['Avg_Ticket'].mean()
    print(f"Media de Avg_Ticket: {median_ticket}")

    # Clasificación basada en la mediana
    customers_summary['ticket_classification'] = customers_summary['Avg_Ticket'].apply(
        lambda x: 'high_ticket' if x >= median_ticket else 'low_ticket'
    )

    return customers_summary

def save_results(customers_summary):
    customers_summary.to_csv('/home/snparada/Spacionatural/Data/Historical/historic_customer_prestashop_classification.csv', index=False)

def main():
    order_detailed_df, orders_df, customers_df = load_data()
    combined_df = prepare_data(orders_df, customers_df, order_detailed_df)
    customers_summary = calculate_customer_metrics(combined_df)
    customers_summary = classify_monetary_by_pareto(customers_summary)
    customers_summary = classify_avg_ticket(customers_summary)
    customers_summary = customers_summary[['email', 'firstname', 'lastname', 'Num_Orders', 'Labels', 'Last_Purchase', 'ticket_classification', 'monetary_classification']]
    save_results(customers_summary)

if __name__ == "__main__":
    main()
