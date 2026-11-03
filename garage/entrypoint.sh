#!/bin/sh
# Start garage, then bootstrap (idempotent): node layout, access key, buckets.
# Each step tolerates "already done" so restarts are safe.
set -e

garage server &
SERVER=$!

until garage status >/dev/null 2>&1; do sleep 1; done

NODE=$(garage node id -q | cut -d@ -f1)
if garage layout show | grep -q "No nodes currently have a role"; then
    garage layout assign -z dc1 -c 10G "$NODE"
    garage layout apply --version 1
fi

garage key import --yes -n pipeline "$S3_ACCESS_KEY" "$S3_SECRET_KEY" 2>/dev/null || true
for bucket in $S3_BUCKETS; do
    garage bucket create "$bucket" 2>/dev/null || true
    garage bucket allow --read --write --owner "$bucket" --key "$S3_ACCESS_KEY"
done

touch /tmp/ready
wait $SERVER
