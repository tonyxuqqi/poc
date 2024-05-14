from store import Store
import json

class SqlStore(Store):
    def __init__(self, table_id, conn):
        self.conn = conn
        self.table_id = table_id

    async def insert(self, key, json_value, index_keys):
        async with self.conn.cursor() as cur:
            sql = f"INSERT INTO {self.table_id} (name, json) VALUES (%s, %s)"
            val = (key.encode(), json_value.encode())
            await cur.execute(sql, val)
        await self.conn.commit()

    async def batch_insert(self, keys, json_values, index_keys_slice):
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        async with self.conn.cursor() as cur:
            sql = f"INSERT INTO {self.table_id} (name, json) VALUES (%s, %s)"
            val = [(key.encode(), json_value.encode()) for key, json_value in zip(keys, json_values)]
            await cur.executemany(sql, val)
        await self.conn.commit()