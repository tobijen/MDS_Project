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

def migrate_result(migrate_ids):

    result_id = migrate_ids["resultId"]
    constructor_id = migrate_ids["constructorId"]
    race_id = migrate_ids["raceId"]
    driver_id = migrate_ids["driverId"]

    try:
        # Fetch the author document from 'dev' scope
        results_query = f"SELECT * FROM `mds_project`.`dev`.`results` WHERE `resultId`={result_id}"
        results_result = cluster.query(results_query)
        result_rows = results_result.rows()  # Adjust as per your document structure
        # Print the data
        if not result_rows:
            print("No results found.")
            return None
        
        for row in result_rows:
            results_data = row
            print(f"Fetched results data: {row}")

        # Fetch related book documents from 'dev' scope
        driver_query = f"SELECT * FROM `mds_project`.`dev`.`drivers` WHERE driverId={driver_id}"
        driver_result = cluster.query(driver_query)
        driver_rows = driver_result.rows()  # Adjust as per your document structure
        # Print the data
        if not driver_rows:
            print("No results found.")
            return None
        
        for row in driver_rows:
            driver_data = row
            print(f"Fetched driver data: {row}")


        # Fetch related book documents from 'dev' scope
        race_query = f"SELECT * FROM `mds_project`.`dev`.`races` WHERE raceId={race_id}"
        race_result = cluster.query(race_query)
        race_rows = race_result.rows()  # Adjust as per your document structure
        # Print the data
        if not race_rows:
            print("No results found.")
            return None
        
        for row in race_rows:
            race_data = row
            print(f"Fetched race data: {row}")

        # Fetch related book documents from 'dev' scope
        constructor_query = f"SELECT * FROM `mds_project`.`dev`.`constructors` WHERE constructorId={constructor_id}"
        constructor_result = cluster.query(constructor_query)
        constructor_rows = constructor_result.rows()  # Adjust as per your document structure
        # Print the data
        if not constructor_rows:
            print("No results found.")
            return None
        
        for row in constructor_rows:
            constructors_data = row
            print(f"Fetched constructor data: {row}")

        # Fetch related book documents from 'dev' scope
        pitstop_query = f"SELECT * FROM `mds_project`.`dev`.`pit_stops` WHERE driverId={driver_id} and raceId={race_id}"
        pitstop_result = cluster.query(pitstop_query)
        pitstop_rows = pitstop_result.rows()  # Adjust as per your document structure
        # Print the data
        if not pitstop_rows:
            print("No results found.")
            return None
     
        for row in pitstop_rows:
            print(f"Fetched pit stops data: {row}")

        """ 
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
        """
    except CouchbaseException as e:
        logging.error(f"Error during migration: {e}")

# Example usage
migrate_result({'constructorId': 51, 'driverId': 855, 'raceId': 1108, 'resultId': 26040})

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
#migrate_all_results()