from store import Store
from schema import columns_str
from schema import columns_format_pg
from schema import index_column
import json

class PgStore(Store):
    def __init__(self, table_id, conn):
        self.conn = conn
        self.table_id = table_id

    async def insert(self, key, json_value, index_keys):
        json_value = json.dumps(json_value)
        sql = f"INSERT INTO {self.table_id} ({columns_str}) VALUES ({columns_format_pg})"
        val = json_value
        await self.conn.execute(sql, *val)

    async def batch_insert(self, keys, json_values, index_keys_slice):
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        sql = f"INSERT INTO {self.table_id} ({columns_str}) VALUES ({columns_format_pg})"
        val = [(json_value,) for json_value in json_values]
        await self.conn.executemany(sql, val)
    
    async def get_by(self, index_key):
        sql = f"select * from {self.table_id} where {index_column} = $1"
        await self.conn.fetch(sql, index_key)
    
    async def multi_get_by(self, index_keys):
        sql = f"select name from {self.table_id} where {index_column} = ANY($1) limit 100"
        await self.conn.fetch(sql, index_keys)