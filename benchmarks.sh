#/bin/bash

set -e -u

DB_DIR=/mnt/data
DB_SIZE=158G
ROWS_PER_USER=3000
GENERATOR=./load-generator/src/load-generator
export LD_LIBRARY_PATH=/usr/local/lib

run_test() {
	local users="$1"
	local commit_size="$2"
	local sync_interval="$3"
	rm -f "$DB_DIR"/data.mdb "$DB_DIR"/lock.mdb
	"$GENERATOR" "$DB_DIR" "$DB_SIZE" "$users" "$ROWS_PER_USER" "$commit_size" "$sync_interval"
}

for users in 125000 500000 1000000 2000000 ; do
	for commit_size in 1 32 1024 ; do
		for sync_interval in -1 0 1 ; do
			run_test "$users" "$commit_size" "$sync_interval"
		done
	done
done
