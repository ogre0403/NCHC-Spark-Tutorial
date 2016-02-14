#!/bin/bash

CURRENT_DIR="$(dirname "$0")"

#rm /tmp/fake.log
#tail -f /tmp/fake.log  | nc -lk 5555 &

while read data; do
    echo "$data" >> /tmp/fake.log
    sleep .$[ ( $RANDOM % 10 ) + 1]
done < $CURRENT_DIR/access_log 



