#!/bin/bash
# Terminal 1: Node A
# Unmount and clean up
fusermount -uz /tmp/ditto_a 2>/dev/null
fusermount -uz /tmp/ditto_b 2>/dev/null
rm -rf /tmp/ditto_a /tmp/ditto_b /tmp/db_a /tmp/db_b
mkdir -p /tmp/ditto_a /tmp/ditto_b /tmp/db_a /tmp/db_b

echo "Pre-building..."
cargo build

echo "Starting Node A..."
cargo run -- --mount /tmp/ditto_a --db /tmp/db_a > node_a.log 2>&1 &
PID_A=$!
sleep 5

echo "Starting Node B..."
cargo run -- --mount /tmp/ditto_b --db /tmp/db_b > node_b.log 2>&1 &
PID_B=$!
sleep 15 # Wait for MDNS discovery

# The demo
echo "Writing 'hello from node A' to /tmp/ditto_a/test.txt"
echo "hello from node A" > /tmp/ditto_a/test.txt
sleep 10

echo "Checking /tmp/ditto_a for files:"
ls -R /tmp/ditto_a

echo "Reading from Node B /tmp/ditto_b/test.txt:"
ls -R /tmp/ditto_b
RESULT=$(cat /tmp/ditto_b/test.txt 2>/dev/null)
echo "Result: $RESULT"

# Cleanup
kill $PID_A $PID_B
wait $PID_A $PID_B 2>/dev/null
fusermount -uz /tmp/ditto_a 2>/dev/null
fusermount -uz /tmp/ditto_b 2>/dev/null

if [ "$RESULT" == "hello from node A" ]; then
    echo "SUCCESS: Sync worked!"
else
    echo "FAILURE: Sync failed or timed out."
    echo "Check node_a.log and node_b.log for details."
fi
