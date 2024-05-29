import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
from azure.cosmos.partition_key import PartitionKey
from store import Store
class CosmosStore(Store):
    def __init__(self, container):
        self.container = container

    async def insert(self, key, json_value, index_keys):
        # Store the original JSON value with the given key
        await self.container.create_item(body=json_value)
    
    async def batch_insert(self, keys, json_values, index_keys_slice):
        for json_value in json_values:
            await self.container.create_item(body=json_value)
    
    async def get_by(self, index_key):
        query = f"SELECT * FROM c WHERE c.name = '{index_key}'"
        items = list(self.container.query_items(query=query, enable_cross_partition_query=True))
        return items
    
    async def multi_get_by(self, index_keys):
        query = f"SELECT * FROM c WHERE c.name in ({index_keys})"
        items = list(self.container.query_items(query=query, enable_cross_partition_query=True))
        return items