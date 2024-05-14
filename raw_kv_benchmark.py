import asyncio
import tikv_client
import threading
import random
import string
import time

from tikv_client.asynchronous import RawClient

# Global variables to track throughput
total_keys_inserted = 0
all_tasks_done = asyncio.Event()
start_time = None
end_time = None

# Function to generate random data for insertion
def generate_random_data():
    return ''.join(random.choices(string.ascii_letters, k=5))

def generate_random_key():
    return ''.join(random.choices(string.ascii_letters, k=5))

def generate_key_value_pairs(thread_id):
    key_value_pairs = {}
    for i in range(100):
        key = f"key_{thread_id}_{i}_{generate_random_key()}"
        value = generate_random_data()
        key_value_pairs[key.encode()] = value.encode()
    return key_value_pairs

# Function to insert data into the database
async def insert_data(thread_id):
    global total_keys_inserted
    # Connect to the database
    client = await RawClient.connect(["192.168.1.232:33815"])
    for j in range(1000):
        kv_pairs = generate_key_value_pairs(thread_id)
        await client.batch_put(kv_pairs) 
        total_keys_inserted += len(kv_pairs)
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
