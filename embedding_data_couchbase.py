from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
#from couchbase.collection import QueryOptions
from couchbase.exceptions import CouchbaseException
import logging

# Setup logging for better traceability
logging.basicConfig(level=logging.INFO)

# Connect to the Couchbase cluster
cluster = Cluster('couchbase://localhost', ClusterOptions(PasswordAuthenticator('Einalem_Saibot', 'Einalem_Saibot')))

# Access the 'dev' and 'public' scopes
bucket = cluster.bucket('mds_project')

dev_scope = bucket.scope('dev')
dev_collection_results = dev_scope.collection('results')  # Replace 'documents' with your actual collection name
dev_collection_drivers = dev_scope.collection('drivers')  # Replace 'documents' with your actual collection name
dev_collection_races = dev_scope.collection('races')  # Replace 'documents' with your actual collection name
dev_collection_races = dev_scope.collection('pit_stops')  # Replace 'documents' with your actual collection name

public_bucket = cluster.bucket('mds_project')
public_scope = public_bucket.scope('public')
public_collection = public_scope.collection('documents')  # Replace 'documents' with your actual collection name

""" def migrate_result(result_id, driver_id):
    try:
        # Fetch the author document from 'dev' scope
        results_query = f"SELECT * FROM `my_bucket`.`dev`.`results` WHERE `resultid`='{result_id}'"
        results_result = cluster.query(results_query)
        result_data = results_result.rows()[0]['documents']  # Adjust as per your document structure
        logging.info(f"Fetched author data: {result_data}")

        # Fetch related book documents from 'dev' scope
        driver_query = f"SELECT * FROM `my_bucket`.`dev`.`drivers` WHERE `driverid`='{driver_id}'"
        books_result = cluster.query(driver_query)
        books_data = [row['documents'] for row in books_result]  # Adjust as per your document structure
        logging.info(f"Fetched books data: {books_data}")

        # Embed books into the author document
        author_data['books'] = [
            {"book_id": book['book_id'], "title": book['title']}
            for book in books_data
        ]

        # Insert the new document into 'public' scope
        new_doc_id = author_data['author_id']
        public_collection.upsert(new_doc_id, author_data)
        logging.info(f"Inserted document into 'public' scope: {new_doc_id}")

        # Optionally, remove the old book documents from 'dev' scope
        for book in books_data:
            old_doc_id = book['book_id']
            dev_collection.remove(old_doc_id)
            logging.info(f"Removed old book document from 'dev' scope: {old_doc_id}")

        # Optionally, remove the original author document from 'dev' scope
        dev_collection.remove(author_id)
        logging.info(f"Removed original author document from 'dev' scope: {author_id}")

    except CouchbaseException as e:
        logging.error(f"Error during migration: {e}") """

# Example usage
#migrate_author('author::1')

# Function to migrate multiple authors
def migrate_all_results():
    try:
        # Fetch all author IDs from 'dev' scope
        migrate_ids_query = "SELECT resultId, driverId, raceId, constructorId FROM `mds_project`.`dev`.`results`"
        migrate_ids_result = cluster.query(migrate_ids_query)
        migrate_ids = [row for row in migrate_ids_result]
        logging.info(f"Fetched author IDs: {migrate_ids}")

        # Migrate each author
        for ids in migrate_ids:
            #migrate_result(author_id)
            logging.info(f"Completed migration for author: {ids}")

    except CouchbaseException as e:
        logging.error(f"Error during migration: {e}")

# Example usage for migrating all authors
migrate_all_results()