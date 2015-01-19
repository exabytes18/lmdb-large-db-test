#/bin/bash

set -e -u

DB_DIR=/mnt/data
DB_SIZE=158G
ROWS_PER_USER=3000
GENERATOR=./data-generator/src/data-generator
export LD_LIBRARY_PATH=/usr/local/lib

run_test() {
	local users="$1"
	local commit_size="$2"
	local sync_interval="$3"
	rm -f "$DB_DIR"/data.mdb "$DB_DIR"/lock.mdb
	"$GENERATOR" "$DB_DIR" "$DB_SIZE" "$users" "$ROWS_PER_USER" "$commit_size" "$sync_interval"
}

mkdir -p results

users=125000
for commit_size in 1 32 1024 ; do
	run_test "$users" "$commit_size" -1 > results/"u$users-b$commit_size-nosync.txt"
	run_test "$users" "$commit_size" 1 > results/"u$users-b$commit_size-periodicsync.txt"
done
