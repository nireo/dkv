#!/bin/bash

set -e

trap 'killall dkv' SIGINT

cd $(dirname $0)

killall dkv || true
sleep 0.1

go install -v


dkv -db=sh1.db -addr=localhost:8080 -shard=sh1 &
dkv -db=sh2.db -addr=localhost:8081 -shard=sh2 &

wait
