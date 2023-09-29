import pandas as pd
from dags.helper import save_to_lake, extract_from_customers, extract_from_merchants, extract_from_orders

def extract_customers_to_lake():
    '''task: extract and load customers documents from MongoDB to datalake'''
    print('Extracting from Mongo')
    customers_dataset = extract_from_customers()
    df = pd.DataFrame(customers_dataset)
    print('Saving to datalake')
    save_to_lake(df, 'customers')

def extract_orders_to_lake():
    '''task: extract and load orders documents from MongoDB to datalake'''
    print('Extracting from Mongo')
    orders_dataset = extract_from_orders()
    df = pd.DataFrame(orders_dataset)
    print('Saving to datalake')
    save_to_lake(df, 'orders')

def extract_merchants_to_lake():
    '''task: extract and load merchants documents from MongoDB to datalake'''
    print('Extracting from Mongo')
    merchants_dataset = extract_from_merchants()
    df = pd.DataFrame(merchants_dataset)
    print('Saving to datalake')
    save_to_lake(df, 'merchants')
