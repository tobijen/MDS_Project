from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
#from couchbase.collection import QueryOptions
from couchbase.exceptions import CouchbaseException
import logging
import json

# Setup logging for better traceability
logging.basicConfig(level=logging.INFO)

# Read the JSON file
json_file_path = './json_data/output.json'
try:
    with open(json_file_path, 'r') as f:
        json_data = json.load(f)
        logging.info(f'Successfully read JSON data from {json_file_path}.')
except FileNotFoundError:
    logging.error(f'File not found: {json_file_path}')
    exit(1)
except json.JSONDecodeError as e:
    logging.error(f'Error decoding JSON: {e}')
    exit(1)

# Connect to the Couchbase cluster
cluster = Cluster('couchbase://localhost', ClusterOptions(PasswordAuthenticator('Einalem_Saibot', 'Einalem_Saibot')))

# Access the 'dev' and 'public' scopes
bucket = cluster.bucket('mds_project')

public_scope = bucket.scope('public')
public_collection_results_final = public_scope.collection('f1_results_embedded')  # Replace 'documents' with your actual collection name



# Function to insert JSON data into Couchbase collection
def insert_data_to_couchbase(collection, json_data):

    for idx, record in enumerate(json_data):
        document_id = f'{record.get("id", "doc")}_{idx}'  # Replace "id" with the key for unique ID in your data
        try:
            collection.insert(document_id, record)
            print(f'Document {document_id} inserted successfully.')
        except Exception as e:
            print(f'Failed to insert document {document_id}: {e}')

insert_data_to_couchbase(public_collection_results_final, json_data)