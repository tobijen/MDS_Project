import os
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.collections import CollectionManager, CollectionSpec
import csv
import json
import datetime

# Configure your Couchbase connection
COUCHBASE_CLUSTER = 'couchbase://localhost'  # Replace with your Couchbase cluster address
BUCKET_NAME = 'mds_project'  # Replace with your bucket name
SCOPE_NAME = 'dev'  # Replace with your scope name
USERNAME = 'Einalem_Saibot'  # Replace with your username
PASSWORD = 'Einalem_Saibot'  # Replace with your password

# Establish connection to Couchbase
cluster = Cluster(COUCHBASE_CLUSTER, ClusterOptions(
    PasswordAuthenticator(USERNAME, PASSWORD)))
bucket = cluster.bucket(BUCKET_NAME)
collection_manager = bucket.collections()
scope = bucket.scope(SCOPE_NAME)

def infer_type(value):
    """Infer the type of a value and convert it."""
    if value == '':
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if value.lower() in ['true', 'false']:
        return value.lower() == 'true'
    return value

def csv_to_json(csv_file_path):
    data_list = []
    with open(csv_file_path, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            converted_row = {key: infer_type(value) for key, value in row.items()}
            data_list.append(converted_row)
    return data_list

# Function to insert JSON data into Couchbase collection
def insert_data_to_couchbase(collection, json_data):

    for idx, record in enumerate(json_data):
        document_id = f'{record.get("id", "doc")}_{idx}'  # Replace "id" with the key for unique ID in your data
        try:
            collection.insert(document_id, record)
            print(f'Document {document_id} inserted successfully.')
        except Exception as e:
            print(f'Failed to insert document {document_id}: {e}')

# Function to create a new collection
def create_collection(scope, collection_name):
    collection_spec = CollectionSpec(collection_name=collection_name, scope_name=SCOPE_NAME)
    try:
        collection_manager.create_collection(collection_spec)
        print(f'Collection {collection_name} created successfully.')
    except Exception as e:
        print(f'Failed to create collection {collection_name}: {e}')

# Function to get a reference to the collection
def get_collection(scope, collection_name):
    collection_spec = CollectionSpec(collection_name=collection_name, scope_name=SCOPE_NAME)
    try:
        return collection_spec
    except Exception as e:
        print(f'Failed to get collection {collection_name}: {e}')
        return None

# Function to iterate over CSV files in a folder and process them
def process_csv_folder(folder_path):
    for filename in os.listdir(folder_path):
        print()
        if filename.endswith('.csv'):
            collection_name = os.path.splitext(filename)[0]  # Use filename without extension as collection name
            print(f'Processing file: {filename}')
            
            # Create the collection for this CSV file
            create_collection(scope, collection_name)
            
            # Get the newly created collection
            new_collection = get_collection(scope, collection_name)
            print(new_collection.name)
            new_collection_name = new_collection.name
            get_new_collection = scope.collection(new_collection_name)
            
            if new_collection is not None and collection_name == 'drivers' or collection_name == 'races':
                print("INSERT")
                file_path = os.path.join(folder_path, filename)
                json_data = csv_to_json(file_path)
                insert_data_to_couchbase(get_new_collection, json_data)
            else:
                print(f'Collection {collection_name} not found.')

# Folder containing CSV files
csv_folder_path = 'csv_data'  # Replace with the path to your folder containing CSV files

# Process and insert CSV data from folder
process_csv_folder(csv_folder_path)