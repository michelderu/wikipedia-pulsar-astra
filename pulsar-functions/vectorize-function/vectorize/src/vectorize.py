from pulsar import Function #  import the Function module from Pulsar
import json

# The VectorizeFunction takes a JSON input and adds a '$vectorize' field to it for use in GenAI
# - Takes the 'content' field and adds a '$vectorize' field with that content
# 
class VectorizeFunction(Function):
    def __init__(self):
        pass

    def process(self, input, context):
        payload = json.loads(input)
        try:
            # Create a new field in the payload with the content to vectorize
            payload["$vectorize"] = payload["content"]
            # Add a metadata field in order for the collection to be used in Langflow
            payload["metadata"] = {"source": "Pulsar"}
        except KeyError:
            context.get_logger().error("Key 'content' not found in payload")
        except Exception as e:
            context.get_logger().error(f"An error occurred: {e}")
        return json.dumps(payload)