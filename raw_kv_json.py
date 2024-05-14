
from tikv_client.asynchronous import RawClient
from store import Store
import json

class RawKVStore(Store):
    def __init__(self, table_id, client: RawClient):
        self.client = client
        self.table_id = table_id

    async def insert(self, key, json_value, index_keys):
        json_value = json.dumps(json_value)
        # Store the original JSON value with the given key
        encoded_key = f"t_{self.table_id}_r{key}".encode()
        await self.client.put(encoded_key, json_value.encode())
        # Store the indexed keys with the same JSON value
        for index_key in index_keys:
            encoded_index_key = f"t_{self.table_id}_i{index_key}_r{key}".encode()
            await self.client.put(encoded_index_key, encoded_key)
    
    async def batch_insert(self, keys, json_values, index_keys_slice):
        pairs = {}
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        for i in range(len(keys)):
            key = keys[i]
            json_value = json_values[i]
            index_keys = index_keys_slice[i]
            encoded_key = f"t_{self.table_id}_r{key}".encode()
            pairs[encoded_key] = json_value.encode()
            for index_key in index_keys:
                encoded_index_key = f"t_{self.table_id}_i{index_key}_r{key}".encode()
                pairs[encoded_index_key] = encoded_key
        await self.client.batch_put(pairs)

    async def get(self, key):
        encoded_key = f"t_{self.table_id}_r{key}".encode()
        json_value = await self.client.get(encoded_key)
        return json_value

    async def get_by(self, index_key):
        encoded_index_key = f"t_{self.table_id}_i{index_key}".encode()
        encoded_key = await self.get(encoded_index_key)
        json_value = await self.get(encoded_key)
        return json_value
