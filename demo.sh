#!/bin/bash
fusermount -uz /tmp/ditto_a 2>/dev/null
fusermount -uz /tmp/ditto_b 2>/dev/null
rm -rf /tmp/ditto_a /tmp/ditto_b /tmp/db_a /tmp/db_b
mkdir -p /tmp/ditto_a /tmp/ditto_b /tmp/db_a /tmp/db_b

echo "Pre-building..."
cargo build --release

echo "Starting Node A..."
./target/release/dittofs --mount /tmp/ditto_a --db /tmp/db_a > node_a.log 2>&1 &
PID_A=$!
sleep 5

echo "Starting Node B..."
./target/release/dittofs --mount /tmp/ditto_b --db /tmp/db_b > node_b.log 2>&1 &
PID_B=$!

echo "Waiting 30s for mDNS discovery and gossipsub peer connection..."
sleep 30

echo "Writing test file on Node A..."
echo "hello from node A" > /tmp/ditto_a/test.txt

echo "Polling Node B for sync (up to 60s)..."
RESULT=""
for i in $(seq 1 30); do
    RESULT=$(cat /tmp/ditto_b/test.txt 2>/dev/null)
    if [ "$RESULT" = "hello from node A" ]; then
        echo "Synced after $((i * 2)) seconds"
        break
    fi
    sleep 2
done

echo "Node A files: $(ls /tmp/ditto_a)"
echo "Node B files: $(ls /tmp/ditto_b)"

kill $PID_A $PID_B 2>/dev/null
wait $PID_A $PID_B 2>/dev/null
fusermount -uz /tmp/ditto_a 2>/dev/null
fusermount -uz /tmp/ditto_b 2>/dev/null

if [ "$RESULT" = "hello from node A" ]; then
    echo "SUCCESS: Sync worked!"
    exit 0
else
    echo "FAILURE: Sync did not complete within 60 seconds."
    echo "--- Node A log ---"
    cat node_a.log
    echo "--- Node B log ---"
    cat node_b.log
    exit 1
fi
