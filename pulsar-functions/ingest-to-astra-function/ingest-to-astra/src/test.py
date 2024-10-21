import json
from astrapy import DataAPIClient

class Logger:
    def info(self, message):
        print(message)

    def debug(self, message):
        print(message)

    def warn(self, message):
        print(message)

    def error(self, message):
        print(message)

class Context:
    def __init__(self):
        pass

    def message_id(self):
        return "123"

    def get_user_config_value(self, key):
        if key == "astra_db_application_token":
            return "AstraCS:SIeROXAHARnQoTZfzEpjXPcU:edf6a647076772db8436d0501b55f70799b9562a145935c8bcacac817621ae9b"
        elif key == "astra_db_api_endpoint":
            return "https://2b3b5531-ef08-4c06-ab7c-b69087bef9b9-us-east-2.apps.astra.datastax.com"
        elif key == "collection":
            return "wiki_stream"
        return "dummy"  

    def get_logger(self):
        return Logger() # dummy logger

context = Context()

input = """{
   "title":"Republic of the Rif",
   "url":"https://en.wikipedia.org/wiki/Republic_of_the_Rif",
   "content":"The Republic of the Rif (Arabic: جمهورية الريف Jumhūriyyatu r-Rīf) was a confederate republic in the Rif, Morocco, that existed between 1921 and 1926. It was created in September 1921, when a coalition of Rifians and Jebala led by Abd el-Krim revolted in the Rif War against the Spanish protectorate in Morocco. The French would intervene on the side of Spain in the later stages of the conflict. A protracted struggle for independence killed many Rifians and Spanish–French soldiers, and witnessed the use of chemical weapons by the Spanish army—their first widespread deployment since the end of the World War I. The eventual Spanish–French victory was owed to the technological and manpower advantages despite their lack of morale and coherence.  Following the war's end, the Republic was ultimately dissolved in 1926."
}"""

data = json.loads(input)
# Add the $vectorize field to the data
data["$vectorize"] = data["content"]

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
   context.get_logger().info(f"Data with message id {context.message_id()} successfully inserted into Astra DB collection {collection.name}.")
except Exception as e:
   context.get_logger().error(f"Error inserting data into Astra DB collection: {e}")
   raise
