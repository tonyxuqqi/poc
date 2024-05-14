import asyncio
import tikv_client
import threading
import random
import string
import time
from faker import Faker
import random
import json


from tikv_client.asynchronous import RawClient
from raw_kv_json import RawKVStore

# Global variables to track throughput
total_keys_inserted = 0
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
    
    # Generate a random age between 18 and 80
    age = random.randint(18, 80)
    
    # Construct and return the JSON object
    json_content = {"name": name, "age": age, "city": city}
    return json_content


# Function to insert data into the database
async def insert_data(thread_id):
    global total_keys_inserted
    # Connect to the database
    client = await RawClient.connect(["192.168.1.232:33815"])
    store = RawKVStore(100, client)
    batch_size = 10
    for j in range(10000):
        # Create a new transaction
        keys = [f"{thread_id}_{generate_random_key()}" for _ in range(batch_size)]
        json_content = [generate_random_json() for _ in range(batch_size)]
        json_content_str = [json.dumps(json_obj) for json_obj in json_content]
        index_keys = [[json_obj["name"]] for json_obj in json_content]
        await store.batch_insert(keys, json_content_str, index_keys)
        total_keys_inserted += batch_size
    # Check if all tasks are done
    if all_tasks_done.is_set():
        all_tasks_done.clear()  # Clear the event if set

# Function to periodically measure throughput
async def measure_throughput():
    global total_keys_inserted
    global start_time
    global end_time

    prev_keys_inserted = 0
    start_time = time.time()
    while True:
        await asyncio.sleep(1)  # Measure throughput every second
        keys_inserted_this_second = total_keys_inserted - prev_keys_inserted
        prev_keys_inserted = total_keys_inserted
        print(f"Throughput: {keys_inserted_this_second} keys/second")
        # Check if all tasks are done
        if all_tasks_done.is_set():
            end_time = time.time()
            break

# Main function to spawn tasks and execute insertions
async def main():
    # Number of tasks to spawn
    num_tasks = 64

    # Create and start tasks
    tasks = []
    for i in range(num_tasks):
        task = asyncio.create_task(insert_data(i))
        tasks.append(task)

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

if __name__ == "__main__":
    asyncio.run(main())
