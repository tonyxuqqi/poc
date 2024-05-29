#!/bin/bash

# Launch 10 instances of Python 3
for ((i=1; i<=10; i++)); do
    python3 json_benchmark.py tidb &  # Replace "your_script.py" with the Python script you want to run
done

# Optionally, wait for all instances to finish
wait

