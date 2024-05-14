
from tikv_client.asynchronous import TransactionClient
from store import Store
import json

class KVStore(Store):
    def __init__(self, table_id, client: TransactionClient):
        self.client = client
        self.table_id = table_id

    async def insert(self, key, json_value, index_keys):
        json_value = json.dumps(json_value)
        # Store the original JSON value with the given key
        txn = await self.client.begin(pessimistic=False)
        encoded_key = f"t_{self.table_id}_r{key}".encode()
        await txn.put(encoded_key, json_value.encode())
        # Store the indexed keys with the same JSON value
        for index_key in index_keys:
            encoded_index_key = f"t_{self.table_id}_i{index_key}_r{key}".encode()
            await txn.put(encoded_index_key, encoded_key)
        # Commit the transaction
        await txn.commit()
    
    async def batch_insert(self, keys, json_values, index_keys_slice):
        json_content_str = [json.dumps(json_obj) for json_obj in json_values]
        txn = await self.client.begin(pessimistic=False)
        for i in range(len(keys)):
            key = keys[i]
            json_value = json_content_str[i]
            index_keys = index_keys_slice[i]
            encoded_key = f"t_{self.table_id}_r{key}".encode()
            await txn.put(encoded_key, json_value.encode())
            for index_key in index_keys:
                encoded_index_key = f"t_{self.table_id}_i{index_key}_r{key}".encode()
                await txn.put(encoded_index_key, encoded_key)
        await txn.commit()

    async def get(self, key):
        txn = await self.client.begin(pessimistic=True)
        encoded_key = f"t_{self.table_id}_r{key}".encode()
        json_value = await txn.get(encoded_key)
        await txn.commit()
        return json_value

    async def get_by(self, index_key):
        txn = await self.client.begin(pessimistic=True)
        encoded_index_key = f"t_{self.table_id}_i{index_key}".encode()
        encoded_key = await txn.get(encoded_index_key)
        json_value = await txn.get(encoded_key)
        await txn.commit()
        return json_value
