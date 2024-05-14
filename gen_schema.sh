#!/bin/bash

# Loop through IDs from 1 to 32
for id in {1..32}; do
    # Construct the file name based on the current ID
    filename="test.sbtest$id-schema.sql"
    sql_content=$(cat <<EOF
CREATE TABLE sbtest$id (
  id INT NOT NULL,
  json JSON,
  name char(64) AS (JSON_EXTRACT(json, '\$.name')),
  job char(64) AS (JSON_EXTRACT(json, '\$.Job')),
  age INT AS (JSON_EXTRACT(json, '\$.age')),
  INDEX name_age (name, age)
);
EOF
)    
    # Copy the source file 'test.sbtest.csv' to the new file name
    echo $sql_content > db_data/"$filename"
done

