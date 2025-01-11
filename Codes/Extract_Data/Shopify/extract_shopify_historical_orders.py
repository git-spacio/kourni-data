import sys, json
import pandas as pd
from decouple import Config, RepositoryEnv

sys.path.append('/home/snparada/Spacionatural/Libraries')
from shopify_lib.orders import ShopifyOrders
from pprint import pprint

dotenv_path = '/home/snparada/Spacionatural/Libraries/shopify_lib/creds/.env'
config = Config(RepositoryEnv(dotenv_path))

shopify_orders_obj = ShopifyOrders()
since_id = 5410729885865
index = 0

while True:
    since_id, all_shopify_orders = shopify_orders_obj.read_all_orders(since_id=since_id, order_status='any')
    key_to_update = 'SINCE_ID'
    with open(dotenv_path, 'r') as file:
        lines = file.readlines()
    with open(dotenv_path, 'w') as file:
        for line in lines:
            if line.startswith(key_to_update):
                file.write(f"{key_to_update}={since_id}\n")
            else:
                file.write(line)
    if len(all_shopify_orders) == 0:
        break

    def main():
        order_id = []
        date = []
        order_number = []
        customer_name = []
        customer_last_name = []
        email = []
        mobile = []
        address = []
        comuna = []
        region = []
        rut = []
        document_type = []
        delivery_name = []
        delivery_amout = []
        invoice_address = []
        invoice_rut = []
        invoice_razon = []
        invoice_giro = []
        invoice_comuna = []
        invoice_region = []
        payment_method = []
        items_number = []
        cart = []

        for shopify_order in all_shopify_orders:
            # order_id
            order_id.append(shopify_order['id'])

            # order_number
            order_number.append(shopify_order['name'])

            # customer_name
            temp_customer_name = ''

            try:
                order_date = shopify_order['processed_at']
            except:
                order_date = ''
            date.append(order_date)

            try:
                temp_customer_name = shopify_order['customer']['first_name']
            except:
                temp_customer_name = ''
            customer_name.append(temp_customer_name)

            # customer_last_name
            temp_customer_last_name = ''
            try:
                temp_customer_last_name = shopify_order['customer']['last_name']
            except:
                temp_customer_last_name = ''
            customer_last_name.append(temp_customer_last_name)
            # email
            temp_email = ''
            try:
                temp_email = shopify_order['customer']['email']
            except:
                temp_email = ''
            email.append(temp_email)
            # mobile
            temp_mobile = ''
            try:
                temp_mobile = shopify_order['customer']['phone']
            except:
                temp_mobile = ''
            mobile.append(temp_mobile)
            # address
            temp_address = ''
            try:
                temp_address = shopify_order['customer']['default_address']['address1']
            except:
                temp_address = ''
            address.append(temp_address)
            # comuna
            temp_comuna = ''
            try:
                temp_comuna = shopify_order['customer']['default_address']['city']
            except:
                temp_comuna = ''
            comuna.append(temp_comuna)
            # region
            temp_region = ''
            try:
                temp_region = shopify_order['customer']['default_address']['province']
            except:
                temp_region = ''
            region.append(temp_region)
            # rut
            temp_rut = ''
            try:
                temp_rut = shopify_order['customer']['default_address']['zip']
            except:
                temp_rut = ''
            rut.append(temp_rut)
            # document_type, invoice_rut, invoice_razon, invoice_giro
            temp_document_type = ''
            temp_invoice_rut = ''
            temp_invoice_razon = ''
            temp_invoice_giro = ''
            for note_attribute in shopify_order['note_attributes']:
                if note_attribute['name'] == 'shoppingcart-tags':
                    temp_document_type = note_attribute['value']
                elif note_attribute['name'] == 'RUT':
                    temp_invoice_rut = note_attribute['value']
                elif note_attribute['name'] == 'razon':
                    temp_invoice_razon = note_attribute['value']
                elif note_attribute['name'] == 'giro':
                    temp_invoice_giro = note_attribute['value']
            document_type.append(temp_document_type)
            invoice_razon.append(temp_invoice_razon)
            invoice_giro.append(temp_invoice_giro)
            # delivery_name
            temp_delivery_name = ''
            try:
                temp_delivery_name = shopify_order['shipping_lines'][0]['title']
            except:
                temp_delivery_name = ''
            delivery_name.append(temp_delivery_name)
            # delivery_amount
            temp_delivery_amount = ''
            try:
                temp_delivery_amount = shopify_order['shipping_lines'][0]['price']
            except:
                temp_delivery_amount = ''
            delivery_amout.append(temp_delivery_amount)
            # invoice_address
            temp_invoice_address = ''
            try:
                temp_invoice_address = shopify_order['billing_address']['address1']
            except:
                temp_invoice_address = ''
            invoice_address.append(temp_invoice_address)
            # invoice_rut
            temp_invoice_rut = ''
            try:
                temp_invoice_rut = shopify_order['billing_address']['zip']
            except:
                temp_invoice_rut = ''
            invoice_rut.append(temp_invoice_rut)
            # invoice_comuna
            temp_invoice_comuna = ''
            try:
                temp_invoice_comuna = shopify_order['billing_address']['city']
            except:
                temp_invoice_comuna = ''
            invoice_comuna.append(temp_invoice_comuna)
            # invoice_region
            temp_invoice_region = ''
            try:
                temp_invoice_region = shopify_order['billing_address']['province']
            except:
                temp_invoice_region = ''
            invoice_region.append(temp_invoice_region)
            # payment_method
            temp_payment_method = ''
            try:
                temp_payment_method = shopify_order['payment_gateway_names'][0]
            except:
                temp_payment_method = ''
            payment_method.append(temp_payment_method)
            # order detail (sku, quantity)
            count = 0
            temp_cart = '['
            for line_item in shopify_order['line_items']:
                count = count + int(line_item['quantity'])
                temp_cart = temp_cart + "{" + str(line_item['sku']) + ', ' + str(line_item['quantity']) + "}"
            temp_cart = f"{temp_cart}]"
            cart.append(temp_cart)
            items_number.append(count)

        data = {
            'order_id': order_id,
            'order_number': order_number,
            'date': date,
            'customer_name': customer_name,
            'customer_last_name': customer_last_name,
            'email': email,
            'mobile': mobile,
            'address': address,
            'comuna': comuna,
            'region': region,
            'rut': rut,
            'document_type': document_type,
            'customer_name': customer_name,
            'delivery_name': delivery_name,
            'delivery_amout': delivery_amout,
            'invoice_address': invoice_address,
            'invoice_rut': invoice_rut,
            'invoice_razon': invoice_razon,
            'invoice_giro': invoice_giro,
            'invoice_comuna': invoice_comuna,
            'invoice_region': invoice_region,
            'payment_method': payment_method,
            'items_number': items_number,
            'cart': cart
        }
        df = pd.DataFrame(data)
        return df

    result = main()
    print(index)
    print(result)

    try:
        if (index == 0):
            result.to_csv('/home/snparada/Spacionatural/Data/Historical/historic_orders_shopify.csv', header=True, index=False)
        else:
            result.to_csv('/home/snparada/Spacionatural/Data/Historical/historic_orders_shopify.csv', mode='a', header=False, index=False)
    except Exception as e:
        print(f"Error al escribir en el archivo: {e}")
    
    index += 1