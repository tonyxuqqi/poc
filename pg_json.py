from store import Store
import json

class PgStore(Store):
    def __init__(self, table_id, conn):
        self.conn = conn
        self.table_id = table_id

    async def insert(self, key, json_value, index_keys):
        json_value = json.dumps(json_value)
        sql = f"INSERT INTO {self.table_id} (name, json) VALUES ($1, $2)"
        val = (key, json_value)
        await self.conn.execute(sql, *val)

    async def batch_insert(self, keys, json_values, index_keys_slice):
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        sql = f"INSERT INTO {self.table_id} (name, json) VALUES ($1, $2)"
        val = [(key, json_value) for key, json_value in zip(keys, json_values)]
        await self.conn.executemany(sql, val)