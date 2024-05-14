#!/bin/bash

# Loop through IDs from 1 to 32
for id in {1..32}; do
    # Construct the file name based on the current ID
    filename="test.sbtest$id.csv"

    # Copy the source file 'test.sbtest.csv' to the new file name
    cp test.sbtest.csv "$filename"

    # Print a message indicating the file has been copied
    echo "Copied test.sbtest.csv to $filename"
done

