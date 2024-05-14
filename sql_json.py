from store import Store
from schema import columns_str
from schema import columns_format_mysql
import json

class SqlStore(Store):
    def __init__(self, table_id, conn):
        self.conn = conn
        self.table_id = table_id

    async def insert(self, key, json_value, index_keys):
        json_values = json.dumps(json_value) 
        async with self.conn.cursor() as cur:
            sql = f"INSERT INTO {self.table_id} ({columns_str}) VALUES ({columns_format_mysql})"
            val = json_value.encode()
            await cur.execute(sql, val)
        await self.conn.commit()

    async def batch_insert(self, keys, json_values, index_keys_slice):
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        async with self.conn.cursor() as cur:
            sql = f"INSERT INTO {self.table_id} ({columns_str}) VALUES ({columns_format_mysql})"
            val = [(json_value.encode(),) for json_value in json_values]
            await cur.executemany(sql, val)
        await self.conn.commit()