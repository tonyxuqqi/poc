from store import Store
from schema import columns_str
from schema import columns_format_mysql
from schema import index_column
import json
class SqlStore(Store):
    def __init__(self, table_id, conn, binary_json):
        self.conn = conn
        self.table_id = table_id
        self.binary_json = binary_json

    async def insert(self, key, json_value, index_keys):
        json_values = json.dumps(json_value) 
        async with self.conn.cursor() as cur:
            sql = f"INSERT INTO {self.table_id} ({columns_str}) VALUES ({columns_format_mysql})"
            if self.binary_json:
                val = json_values.encode()
            else:
                val = json_value
            await cur.execute(sql, val)
        await self.conn.commit()

    async def batch_insert(self, keys, json_values, index_keys_slice):
        json_values = [json.dumps(json_obj) for json_obj in json_values]
        async with self.conn.cursor() as cur:
            sql = f"INSERT INTO {self.table_id} ({columns_str}) VALUES ({columns_format_mysql})"
            if self.binary_json:
                val = [json_value.encode() for json_value in json_values]
            else:
                val = [(json_value,) for json_value in json_values]
            await cur.executemany(sql, val)
        await self.conn.commit()
    
    async def get_by(self, index_key):
        async with self.conn.cursor() as cur:
            sql = f"select * from {self.table_id} where {index_column} = %s"
            await cur.execute(sql, (index_key,))
            # Fetch all rows matching the query
            await cur.fetchall()
    
    async def multi_get_by(self, index_keys):
        async with self.conn.cursor() as cur:
            sql = "select name from {} where {} in ({}) limit 100".format(self.table_id, index_column, ', '.join(['%s'] * len(index_keys)))
            await cur.execute(sql, index_keys)
            # Fetch all rows matching the query
            await cur.fetchall() 