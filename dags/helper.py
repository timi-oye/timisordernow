import boto3
import pandas as pd
from pymongo import MongoClient
from configparser import ConfigParser

config = ConfigParser()
config.read('./.env')

access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
region = config['AWS']['region']
bucket_name = config['S3']['bucket_name']
conn_str = config['DB_CONN']['uri']

raw_test_path = 's3://{}/raw/{}.csv'

def create_bucket():
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region
            }
        )
    except Exception as e:
        print('Unable to create bucket: ', e)



# ============== EXTRACTION - LOADING
def save_to_lake(df, file_name):
    df.to_csv(
        raw_test_path.format(bucket_name, file_name)
        , index=False
        , storage_options={
            'key': access_key
            , 'secret': secret_key
        }
    )



def create_db_client():
    client = MongoClient(conn_str)
    return client

def extract_from_merchants():
    db_client = create_db_client()
    ordernow = db_client.ordernow
    merchants = ordernow.merchants
    return merchants.find()


def extract_from_customers():
    db_client = create_db_client()
    ordernow = db_client.ordernow
    customers = ordernow.customers
    return customers.find()

def extract_from_orders():
    db_client = create_db_client()
    ordernow = db_client.ordernow
    orders = ordernow.orders
    return orders.find()

def read_csv_from_lake(s3_path):
    df = pd.read_csv(s3_path, storage_options={
        'key': access_key
        , 'secret': secret_key
    })
    print(df.head())
    return df

def write_csv_to_lake(s3_path, df):
    df.to_csv(s3_path
              , storage_options={
        'key': access_key
        , 'secret': secret_key
    })
    print(df.head())