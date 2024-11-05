from pulsar import Function #  import the Function module from Pulsar
import json

# The VectorizeFunction takes a JSON input and enables the use of the content in Langflow
# - It keeps 'content' in the main payload
# - It adds a '$vectorize' field with the content to the payload so that it will be picked up by the Vectorize function
# - Takes any other fields and moves them to metadata
# - Adds source metadata to the payload related to the Pulsar context
# 
class VectorizeFunction(Function):
    def __init__(self):
        pass

    def process(self, input, context):
        payload = json.loads(input)

        try:
            # Create the result payload
            result = {}
            # Copy the content to the result
            result["content"] = payload["content"]
            # Create a new field in the payload with the content to vectorize
            result["$vectorize"] = payload["content"]
            # Process the metadata
            # 1. Add a metadata field in order for the collection to be used in Langflow
            metadata = {"source": "Pulsar", "message_id": str(context.get_message_id())}
            # 2. Copy all the non-content fields to metadata
            for key, value in payload.items():
                if key != "content":
                    metadata[key] = value
            result["metadata"] = metadata

            return json.dumps(result)
        except KeyError:
            context.get_logger().error("Key 'content' not found in payload")
        except Exception as e:
            context.get_logger().error(f"An error occurred: {e}")