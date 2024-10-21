from pulsar import Function
import json
from astrapy import DataAPIClient

# The IngestToAstraFunction takes a JSON input and stores it into Astra DB using the Data API
#
class IngestToAstraFunction(Function):
    def __init__(self):
        pass

    def process(self, input, context):
        # Get the data from the input
        data = json.loads(input)

        try:
            # Initialize the client and get a "Database" object
            client = DataAPIClient(context.get_user_config_value('astra_db_application_token'))
            database = client.get_database(context.get_user_config_value('astra_db_api_endpoint'))
        except Exception as e:
            context.get_logger().error(f"Error initializing Astra DB client or getting database: {e}")
            raise

        # Get the collection to store the payload in
        try:
            collection = database.get_collection(context.get_user_config_value('collection'))
        except Exception as e:
            context.get_logger().error(f"Error getting collection from Astra DB: {e}")
            raise

        # Insert the data into the collection
        try:
            collection.insert_one(data)
            context.get_logger().info(f"Data with message id {context.get_message_id()} successfully inserted into Astra DB collection {collection.name}.")
        except Exception as e:
            context.get_logger().error(f"Error inserting data into Astra DB collection: {e}")
            raise
    
        return json.dumps(data)