import pandas as pd
from configparser import ConfigParser

from helper import read_csv_from_lake, write_csv_to_lake

config = ConfigParser()
config.read('.env')

access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
bucket_name = config['S3']['bucket_name']

raw_s3_uri = 's3://{}/raw/{}.csv'
clean_s3_uri = 's3://{}/clean/{}.csv'
agg_s3_uri = 's3://{}/agg/{}/{}.csv'

def extract_city(address):
    '''utility: extract city from address
    :param
        address (str)
    :return
        city (str)
    '''
    return address.split(';')[0] 
def extract_state(address):
    '''utility: extract state from address
    :param
        address (str)
    :return
        state (str)
    '''
    return address.split(';')[1] 


def clean_customers_data():
    '''task: clean customers' dataset'''
    df = read_csv_from_lake(raw_s3_uri.format(bucket_name, 'customers'))
    print('=========================== CUSTOMERS ===========================')
    df['city'] = df['address'].apply(extract_city)
    df['state'] = df['address'].apply(extract_state)

    df['referred_by'] = df['referred_by'].apply(lambda x: str(x) if type(x) == int or type(x) == float else 'null')
    write_csv_to_lake(clean_s3_uri.format(bucket_name, 'customers'), df)


def clean_merchants_data():
    '''task: clean merchants dataset'''
    df = read_csv_from_lake(raw_s3_uri.format(bucket_name, 'merchants'))
    print('=========================== MERCHANTS ===========================')
    print(df.head())
    df['city'] = df['address'].apply(extract_city)
    df['state'] = df['address'].apply(extract_state)

    df['isActive'] = df['isActive'].apply(lambda x: 'true' if x == 'yes' or x == 'true' else 'false')
    df['name'] = df['name'].apply(lambda x: x if x != '' else 'Not Provided')

    write_csv_to_lake(clean_s3_uri.format(bucket_name, 'merchants'), df)


def clean_orders_data():
    '''task: clean orders dataset'''
    df = read_csv_from_lake(raw_s3_uri.format(bucket_name, 'orders'))
    print('=========================== ORDERS ===========================')
    print(df.head())

    df['promoCode'] = df['promoCode'].fillna('None')
    df['orderDate'] = df['orderDate'].apply(lambda x: pd.to_datetime(x, format="%d/%m/%Y"))
    df['month'] = df['orderDate'].dt.month_name()
    df['month_number'] = df['orderDate'].dt.month_name()
    df['year'] = df['orderDate'].dt.year

    write_csv_to_lake(clean_s3_uri.format(bucket_name, 'orders'), df)



# ========================================= TRANSFORMATION


# -------- Customers

def agg_customer_order_rejection():
    '''task: (aggregation) load "above 3 rejections" to lake's "agg" folder'''
    all_customers = read_csv_from_lake(clean_s3_uri.format(bucket_name, 'customers'))
    new_df = all_customers[all_customers['numberOfOrderRejections'] > 3][['firstName', 'lastName', 'numberOfOrderRejections']]
    write_csv_to_lake(agg_s3_uri.format(bucket_name, 'customers', 'customer_order_rejection'), new_df)

def agg_customers_per_state():
    '''task: (aggregation) load "total customers per state" to lake's "agg" folder'''
    all_customers = read_csv_from_lake(clean_s3_uri.format(bucket_name, 'customers'))
    new_df = all_customers[['state', '_id']].groupby(by='state').count()
    new_df.rename({'_id': 'total_customers'}, inplace=True)
    write_csv_to_lake(agg_s3_uri.format(bucket_name, 'customers', 'customers_per_state'), new_df)


# -------- Orders

def agg_total_orders_per_month():
    '''task: (aggregation) load "total orders per month" to lake's "agg" folder'''
    all_orders = read_csv_from_lake(clean_s3_uri.format(bucket_name, 'orders'))
    new_df = all_orders[['_id', 'year', 'month']].groupby(by=['year', 'month']).count()
    new_df.rename({'_id': 'total_orders'}, inplace=True)
    write_csv_to_lake(agg_s3_uri.format(bucket_name, 'orders', 'total_orders_per_month'), new_df)


def agg_orders_list():
    '''task: (listing) load "transformed orders list" to lake's "agg" folder'''
    all_customers = read_csv_from_lake(clean_s3_uri.format(bucket_name, 'customers'))
    all_orders = read_csv_from_lake(clean_s3_uri.format(bucket_name, 'orders'))
    all_merchants = read_csv_from_lake(clean_s3_uri.format(bucket_name, 'merchants'))
    new_df = all_orders[['_id', 'meal', 'merchants', 'orderedBy', 'orderDate', 'promoCode']]\
        .merge(all_merchants, how='left', left_on='merchants', right_on='_id')
    print(new_df)
    new_df = new_df[['_id_x', 'meal', 'name', 'orderedBy', 'orderDate', 'promoCode']]\
        .merge(all_customers[['_id', 'firstName', 'lastName', 'state', 'city', 'registration_date']], how='left', left_on='orderedBy', right_on='_id')\
        [['_id_x', 'meal', 'name', 'orderedBy', 'orderDate', 'promoCode', 'firstName', 'lastName','state', 'city', 'registration_date']]
    print('==================================\n', new_df)
    new_df.rename({'name': 'merchant', '_id_x': 'orderID'}, inplace=True)
    write_csv_to_lake(agg_s3_uri.format(bucket_name, 'orders', 'orders_list'), new_df)
