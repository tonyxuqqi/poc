import asyncio
from aiocassandra import aiosession
from cassandra.query import BatchStatement
import json

from store import Store
class CqlStore(Store):
    def __init__(self, table_name, session):
        self.session = session
        self.table_name = table_name

    async def insert(self, key, json_value, index_keys):
        # Store the original JSON value with the given key
        await self.session.execute_async(f"INSERT INTO {self.table_name} (id, json, name) VALUES ('{key}', '{json_value.encode()}', '{json_value.get('name')}')")
            
    async def batch_insert(self, keys, json_values, index_keys_slice):
        names = [json_value.get('name') for json_value in json_values]
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        batch = BatchStatement()
        prepared = self.session.prepare(f"INSERT INTO {self.table_name} (id, json, name) VALUES (?, ?, ?)")
        for key, json_value, name in zip(keys, json_values, names):
            batch.add(prepared, (key, json_value, name))
        self.session.execute(batch)
    
    async def get_by(self, index_key):
        query = f"SELECT id, JSON json, name FROM {self.table_name} WHERE name = '{index_key}'"
        items = await self.session.execute_async(query)
        return items
    
    async def multi_get_by(self, index_keys):
        query = f"SELECT id, JSON json, name FROM {self.table_name} limit 100 WHERE name in ({', '.join(['%s'] * len(index_keys))})"
        items = await self.session.execute_async(query, index_keys)
        return items
        