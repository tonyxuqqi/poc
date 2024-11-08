import asyncio
import random
import string
import time
from faker import Faker
import random
import json
import aiomysql
import asyncpg

from tikv_client.asynchronous import TransactionClient
from tikv_client.asynchronous import RawClient
from sql_json import SqlStore
from kv_json import KVStore
from raw_kv_json import RawKVStore
from pg_json import PgStore
from cosmos_json import CosmosStore
from cql_json import CqlStore
from azure.cosmos import exceptions, PartitionKey
from azure.cosmos.aio import CosmosClient
from aiocassandra import aiosession
from cassandra.cluster import Cluster

import sys
import uuid

# Global variables to track throughput
total_keys_inserted = 0
total_keys_queried = 0
all_tasks_done = asyncio.Event()
start_time = None
end_time = None

# Function to generate random data for insertion
def generate_random_data():
    return ''.join(random.choices(string.ascii_letters, k=5))

def generate_random_key():
    return ''.join(random.choices(string.ascii_letters, k=10))


# Create an instance of the Faker generator
fake = Faker()

# Generate a large list of unique names and cities
num_names = 100000
num_cities = 10000
names = [fake.first_name() for _ in range(num_names)]
cities = [fake.city() for _ in range(num_cities)]

# Function to generate random JSON content
def generate_random_json():
    # Randomly select a name and city
    name = random.choice(names)
    city = random.choice(cities)
    # Generate a random UUID
    id = str(uuid.uuid4())
    
    # Generate a random age between 18 and 80
    age = random.randint(18, 80)
    
    # Construct and return the JSON object
    json_content = {"id": id, "name": name, "age": age, "city": city}
    return json_content


async def insert_data_common(thread_id, store, batch_size):
    global total_keys_inserted
    for j in range(1000000):
        # Create a new transaction  
        json_content = [generate_random_json() for _ in range(batch_size)]
        keys = [json_obj["id"] for json_obj in json_content]
        index_keys = [[json_obj["name"]] for json_obj in json_content]
        await store.batch_insert(keys, json_content, index_keys)
        total_keys_inserted += batch_size
    
    # Check if all tasks are done
    if all_tasks_done.is_set():
        all_tasks_done.clear()  # Clear the event if set

async def query_data_common(store, batch_size):
    global total_keys_queried
    # Query the data
    for i in range(100000):
        
        await store.multi_get_by(names[i:i+batch_size])
        i += batch_size
        total_keys_queried += 1

      # Check if all tasks are done
    if all_tasks_done.is_set():
        all_tasks_done.clear()  # Clear the event if set

# Function to insert data into the database
async def insert_data_tidb(thread_id):
    # Connect to the database
    # CREATE TABLE `customer`(`id` char(64) primary key GENERATED ALWAYS AS (JSON_UNQUOTE(json_extract(`json`, '$.id'))) STORED, `json` json DEFAULT NULL,  `name` char(64) 
    # GENERATED ALWAYS AS (JSON_UNQUOTE(json_extract(`json`, '$.name'))) STORED, KEY `json_name_1` (name));
    conn = await aiomysql.connect(
        host="192.168.1.232",
        port= 33721,
        user="root",
        password="",
        db="sbtest3"
    )
    store = SqlStore("customer", conn, binary_json=True)
    batch_size = 10
    #await insert_data_common(thread_id, store, batch_size)
    await query_data_common(store,batch_size)
    conn.close()

async def insert_data_mysql(thread_id):
    # Connect to the database
    # CREATE TABLE `customer`(`id` char(64) GENERATED ALWAYS AS (json->>'$.id') STORED primary key, `json` json DEFAULT NULL,  `name` char(64) 
    # GENERATED ALWAYS AS (json->>'$.name') STORED, KEY `json_name_1` (name));
    conn = await aiomysql.connect(
        host="127.0.0.1",
        port= 3317,
        user="root",
        password="",
        db="sbtest3"
    )
    store = SqlStore("customer", conn, binary_json=False)
    batch_size = 10
    #await insert_data_common(thread_id, store, batch_size)
    await query_data_common(store,batch_size)
    conn.close()
    
async def insert_data_crdb(thread_id):
      # Connect to the database
    # CREATE TABLE customer (id char(64) primary key GENERATED ALWAYS AS ((json->>'id')) STORED, json JSON DEFAULT NULL,  name char(64) GENERATED ALWAYS AS ((json->>'name')) STORED);
    # CREATE INDEX ON test2.public.customer (name) STORING (json);
    conn = await asyncpg.connect(
        host="192.168.1.232",
        port= 26257,
        user="root",
        password="",
        database="test2"
    )
    store = PgStore("customer", conn)
    batch_size = 10
    #await insert_data_common(thread_id, store, batch_size) 
    await query_data_common(store, batch_size)
    await conn.close()

async def insert_data_pg(thread_id):
    # Connect to the database
    # CREATE TABLE customer (id char(64) primary key GENERATED ALWAYS AS ((json->>'id')) STORED,  json JSON DEFAULT NULL,  name char(64) GENERATED ALWAYS AS ((json->>'name')) STORED);
    # CREATE INDEX idx_derived_column ON customer (name);
    conn = await asyncpg.connect(
        host="localhost",
        port= 5433,
        user="postgres",
        password="",
        database="test2"
    )
    store = PgStore("customer", conn)
    batch_size = 10
    #await insert_data_common(thread_id, store, batch_size)
    await query_data_common(store, batch_size)
    await conn.close()

async def insert_data_kv(thread_id):
    # Connect to the database
    client = await TransactionClient.connect(["192.168.1.232:33815"])
    store = KVStore(500, client)
    batch_size = 10
    await insert_data_common(thread_id, store, batch_size)
    await query_data_common(store, batch_size)

async def insert_data_raw_kv(thread_id):
    # Connect to the database
    client = await RawClient.connect(["40.76.113.99:2379"])
    store = RawKVStore(300, client)
    batch_size = 10
    await insert_data_common(thread_id, store, batch_size)
    await query_data_common(store, batch_size)

async def insert_data_cosmosdb(thread_id):
    HOST = "https://cosmos-db-free-tier-moray.documents.azure.com:443/"
    MASTER_KEY = "e8uOtCf5VUdUAxIL6h1xLIdHD5WuxeqOqIg1Te7qQt9CXfirKQJYEmfDCHIsroOFw3mvlDGgDWfUACDb0XAaHw=="
    async with CosmosClient(HOST, {'masterKey': MASTER_KEY}) as client:
        # setup database for this sample
        DATABASE_ID = "cosmosdb"
        try:
            db = await client.create_database_if_not_exists(id=DATABASE_ID)
        except exceptions.CosmosResourceExistsError:
            db = await client.get_database_client(DATABASE_ID)
        # setup container for this sample
        try:
                # json_content = {"id": id, "name": name, "age": age, "city": city}
            indexing_policy = {
                "indexingMode": "consistent",
                "includedPaths": [
                {
                    "path": "/*"
                }
                ],
                "excludedPaths": [
                {
                    "path": "/age/?"
                },
                {
                    "path": "/city/?"
                }
                ]
            }
            container = await db.create_container_if_not_exists(id="customer", indexing_policy=indexing_policy, partition_key=PartitionKey(path='/name'))
        except exceptions.CosmosResourceExistsError:
            container = await db.get_container_client("customer")
        batch_size = 10
        store = CosmosStore(container)  
        await insert_data_common(thread_id, store, batch_size)
        await query_data_common(store, batch_size)
        # cleanup database after sample
        try:
           await client.delete_database(db)
        except exceptions.CosmosResourceNotFoundError:
           pass

async def insert_data_cql_json(thread_id):
    # Connect to the database
    cluster = Cluster(["127.0.0.1"], port=19042)
    session = cluster.connect("ns1")
    aiosession(session)
    store = CqlStore("customer", session)
    batch_size = 10
    await insert_data_common(thread_id, store, batch_size)
    session.shutdown()
    cluster.shutdown()


# Function to periodically measure throughput
async def measure_throughput():
    global total_keys_inserted
    global total_keys_queried
    global start_time
    global end_time

    prev_keys_inserted = 0
    prev_keys_queried = 0
    start_time = time.time()
    while True:
        await asyncio.sleep(1)  # Measure throughput every second
        keys_inserted_this_second = total_keys_inserted - prev_keys_inserted
        keys_queries_this_seconcd = total_keys_queried - prev_keys_queried
        prev_keys_inserted = total_keys_inserted
        prev_keys_queried = total_keys_queried
        print(f"Throughput: Insert {keys_inserted_this_second} keys/second, Query {keys_queries_this_seconcd}/keys/second")
        # Check if all tasks are done
        if all_tasks_done.is_set():
            end_time = time.time()
            break

# Main function to spawn tasks and execute insertions
async def main():
    # Number of tasks to spawn
    num_tasks = 1

    # Create and start tasks
    tasks = []
    # Get the store_type value from the command line parameter
    store_type = sys.argv[1]

    # Create and start tasks
    tasks = []
    create_task(num_tasks, tasks, store_type)

    # Start the throughput measurement task
    throughput_task = asyncio.create_task(measure_throughput())

   # Wait for all tasks to finish
    await asyncio.gather(*tasks)

    # Signal that all tasks are done
    all_tasks_done.set()

    # Wait for the throughput measurement to finish
    await throughput_task

    # Calculate and print summary
    total_time = end_time - start_time
    avg_throughput = total_keys_inserted / total_time
    print(f"Total rows inserted: {total_keys_inserted}")
    print(f"Total time taken: {total_time} seconds")
    print(f"Average rows inserted per second: {avg_throughput}")

def create_task(num_tasks, tasks, store_type):
    for i in range(num_tasks):
        if store_type == "tidb":
            task = asyncio.create_task(insert_data_tidb(i))
            tasks.append(task)
        elif store_type == "mysql":
            task = asyncio.create_task(insert_data_mysql(i))
            tasks.append(task)
        elif store_type == "tx_kv":
            task = asyncio.create_task(insert_data_kv(i))
            tasks.append(task)
        elif store_type == "raw_kv":
            task = asyncio.create_task(insert_data_raw_kv(i))
            tasks.append(task)
        elif store_type == "crdb":
            task = asyncio.create_task(insert_data_crdb(i))
            tasks.append(task)
        elif store_type == "pg":
            task = asyncio.create_task(insert_data_pg(i))
            tasks.append(task)
        elif store_type == "cosmosdb":
            task = asyncio.create_task(insert_data_cosmosdb(i))
            tasks.append(task)
        elif store_type == "cql":
            task = asyncio.create_task(insert_data_cql_json(i))
            tasks.append(task)
        else:
            print("Invalid store_type parameter")

if __name__ == "__main__":
    asyncio.run(main())
