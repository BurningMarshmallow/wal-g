#!/bin/sh
set -e -x

export WALE_S3_PREFIX=s3://redisbucket

wal-g stream-fetch LATEST > /tmp/dump.rdb
cat /tmp/dump.rdb

echo "Redis stream-fetch test was successful"
