#include <arpa/inet.h>
#include <errno.h>
#include <lmdb.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "utils.h"

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
			"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

static const int num_user_iteration_patterns = 100;
static const int num_txn_iteration_patterns = 1000;
static const struct timespec reporter_interval = {.tv_sec = 1, .tv_nsec = 0};

struct reporting_data {
	volatile long long int rows_inserted;
	volatile long long int rows_total;
	int *stopping;
	int shutdown_fd;
};


struct sync_data {
	MDB_env *env;
	int interval_in_seconds;
	int *stopping;
	int shutdown_fd;
};


static void * _sync_thread_main(void *ptr) {
	struct sync_data *sync_data = ptr;
	struct timespec
		now,
		delay,
		next_interval,
		interval_in_seconds = {
			.tv_sec = sync_data->interval_in_seconds,
			.tv_nsec = 0
		};
	int rc, max_fd;
	fd_set rfds;

	get_nanotime(&next_interval);
	while (!*sync_data->stopping) {
		timespec_add(&next_interval, &next_interval, &interval_in_seconds);
		mdb_env_sync(sync_data->env, 1);
		while (!*sync_data->stopping) {
			get_nanotime(&now);
			timespec_subtract(&delay, &next_interval, &now);
			if (delay.tv_sec > 0 || delay.tv_nsec > 0) {
				max_fd = 0;
				FD_ZERO(&rfds);

				max_fd = MAX(max_fd, sync_data->shutdown_fd + 1);
				FD_SET(sync_data->shutdown_fd, &rfds);

				rc = pselect(max_fd, &rfds, NULL, NULL, &delay, NULL);
				if (rc == 1) {
					goto done;
				} else if (rc == -1) {
					fprintf(stderr, "problem waiting on pselect(): %s\n", strerror(rc));
					abort();
				}
			} else {
				break;
			}
		}
	}

done:
	return NULL;
}


static void * _reporter_thread_main(void *ptr) {
	struct reporting_data *reporting_data = ptr;
	struct timespec delay, interval_start, interval_end, interval;
	int rc, max_fd;
	long long int interval_start_rows, interval_end_rows, delta;
	double percent_complete, interval_in_seconds, rate, timestamp;
	fd_set rfds;

	while (!*reporting_data->stopping) {
		interval_start_rows = reporting_data->rows_inserted;
		get_nanotime(&interval_start);
		delay = reporter_interval;
		do {
			max_fd = 0;
			FD_ZERO(&rfds);

			max_fd = MAX(max_fd, reporting_data->shutdown_fd + 1);
			FD_SET(reporting_data->shutdown_fd, &rfds);

			rc = pselect(max_fd, &rfds, NULL, NULL, &delay, NULL);
			if (rc == 1) {
				goto done;
			} else if (rc == -1) {
				fprintf(stderr, "problem waiting on pselect(): %s\n", strerror(rc));
				abort();
			}

			interval_end_rows = reporting_data->rows_inserted;
			get_nanotime(&interval_end);
			timespec_subtract(&interval, &interval_end, &interval_start);
			timespec_subtract(&delay, &reporter_interval, &interval);
		} while (!*reporting_data->stopping && (delay.tv_sec > 0 || delay.tv_nsec > 0));

		timestamp = interval_start.tv_sec + interval_start.tv_nsec / 1e9;
		percent_complete = ((double) reporting_data->rows_inserted / reporting_data->rows_total) * 100.;
		delta = interval_end_rows - interval_start_rows;
		interval_in_seconds = interval.tv_sec + interval.tv_nsec / 1e9;
		rate = delta / interval_in_seconds;

		printf("[%5.1lf%%, %.3lf]: inserted %lld rows in %.3lfs; %.3lf rows/sec\n", percent_complete, timestamp, delta, interval_in_seconds, rate);
		fflush(stdout);
	}

done:
	printf("\n");
	return NULL;
}


static void _insert(MDB_env *env, int num_users, int num_txns_per_user, int num_rows_per_commit, int sync_interval_in_seconds, struct timespec *start, struct timespec *end) {
	int i, j, r, rc, t,
		user_id,
		txn_id,
		timestamp,
		**user_iteration_patterns,
		**txn_iteration_patterns,
		user_id_network_order,
		txn_id_network_order,
		timestamp_network_order,
		amount_network_order,
		txn_row_count,
		shutdown_pipe[2],
		stopping = 0;
	float amount;
	char completed, primary_key[8], value[9];
	MDB_dbi dbi;
	MDB_txn *txn;
	MDB_val key, data;
	pthread_t reporter_thread, sync_thread;
	struct reporting_data reporting_data = {
		.rows_inserted = 0,
		.rows_total = num_users * num_txns_per_user,
		.stopping = &stopping
	};
	struct sync_data sync_data = {
		.env = env,
		.interval_in_seconds = sync_interval_in_seconds,
		.stopping = &stopping
	};

	user_iteration_patterns = malloc(sizeof(*user_iteration_patterns) * num_user_iteration_patterns);
	for (i = 0; i < num_user_iteration_patterns; i++) {
		user_iteration_patterns[i] = malloc(sizeof(user_iteration_patterns[i]) * num_users);
		for (j = 0; j < num_users; j++) {
			user_iteration_patterns[i][j] = j + 1;
		}
		for (j = num_users - 1; j > 0; j--) {
			r = rand() % (j + 1);
			t = user_iteration_patterns[i][j];
			user_iteration_patterns[i][j] = user_iteration_patterns[i][r];
			user_iteration_patterns[i][r] = t;
		}
	}

	txn_iteration_patterns = malloc(sizeof(*txn_iteration_patterns) * num_txn_iteration_patterns);
	for (i = 0; i < num_txn_iteration_patterns; i++) {
		txn_iteration_patterns[i] = malloc(sizeof(txn_iteration_patterns[i]) * num_txns_per_user);
		for (j = 0; j < num_txns_per_user; j++) {
			txn_iteration_patterns[i][j] = j + 1;
		}
		for (j = num_txns_per_user - 1; j > 0; j--) {
			r = rand() % (j + 1);
			t = txn_iteration_patterns[i][j];
			txn_iteration_patterns[i][j] = txn_iteration_patterns[i][r];
			txn_iteration_patterns[i][r] = t;
		}
	}

	if (pipe(shutdown_pipe) != 0) {
		fprintf(stderr, "Unable to create shutdown pipe: %s\n", strerror(errno));
		abort();
	}
	if (set_nonblocking(shutdown_pipe[0]) != 0) {
		fprintf(stderr, "problem setting shutdown pipe (read side) to be nonblocking: %s\n", strerror(errno));
		abort();
	}
	if (set_nonblocking(shutdown_pipe[1]) != 0) {
		fprintf(stderr, "problem setting shutdown pipe (write side) to be nonblocking: %s\n", strerror(errno));
		abort();
	}

	reporting_data.shutdown_fd = shutdown_pipe[0];
	rc = pthread_create(&reporter_thread, NULL, _reporter_thread_main, &reporting_data);
	if (rc != 0) {
		fprintf(stderr, "Problem creating reporter_thread: %s\n", strerror(rc));
		abort();
	}

	if (sync_interval_in_seconds > 0) {
		sync_data.shutdown_fd = shutdown_pipe[0];
		rc = pthread_create(&sync_thread, NULL, _sync_thread_main, &sync_data);
		if (rc != 0) {
			fprintf(stderr, "Problem creating sync_thread: %s\n", strerror(rc));
			abort();
		}
	}

	// timer start
	get_nanotime(start);

	txn_row_count = 0;
	E(mdb_txn_begin(env, NULL, 0, &txn));
	E(mdb_open(txn, NULL, 0, &dbi));
	for (i = 0; i < num_txns_per_user; i++) {
		for (j = 0; j < num_users; j++) {
			user_id = user_iteration_patterns[i % num_user_iteration_patterns][j];
			txn_id = txn_iteration_patterns[user_id % num_txn_iteration_patterns][i];

			user_id_network_order = htonl(user_id);
			txn_id_network_order = htonl(txn_id);
			memcpy(primary_key,	    &user_id_network_order, 4);
			memcpy(primary_key + 4, &txn_id_network_order, 4);

			timestamp = user_id + txn_id;
			amount = txn_id + 0.5f;
			completed = timestamp % 2;
			timestamp_network_order = htonl(timestamp);
			amount_network_order = htonl(*((float *)((void *)&amount))); // valid?
			memcpy(value,	  &timestamp_network_order, 4);
			memcpy(value + 4, &amount_network_order, 4);
			memcpy(value + 8, &completed, 1);

			key.mv_size = sizeof(primary_key);
			key.mv_data = primary_key;
			data.mv_size = sizeof(value);
			data.mv_data = value;
			E(mdb_put(txn, dbi, &key, &data, 0));

			txn_row_count++;
			reporting_data.rows_inserted++;
			if (txn_row_count >= num_rows_per_commit) {
				E(mdb_txn_commit(txn));
				E(mdb_txn_begin(env, NULL, 0, &txn));
				txn_row_count = 0;
			}
		}
	}
	E(mdb_txn_commit(txn));
	mdb_close(env, dbi);

	// timer end
	get_nanotime(end);

	stopping = 1;
	if (force_write(shutdown_pipe[1], 0) != 1) {
		fprintf(stderr, "cannot write to shutdown pipe: %s\n", strerror(errno));
		abort();
	}

	rc = pthread_join(reporter_thread, NULL);
	if (rc != 0) {
		fprintf(stderr, "Problem joining reporter thread: %s\n", strerror(rc));
		abort();
	}

	if (sync_interval_in_seconds > 0) {
		rc = pthread_join(sync_thread, NULL);
		if (rc != 0) {
			fprintf(stderr, "Problem joining sync thread: %s\n", strerror(rc));
			abort();
		}
	}

	if (uninterruptable_close(shutdown_pipe[0]) != 0) {
		fprintf(stderr, "problem closing shutdown pipe (read side): %s\n", strerror(errno));
	}
	if (uninterruptable_close(shutdown_pipe[1]) != 0) {
		fprintf(stderr, "problem closing shutdown pipe (read side): %s\n", strerror(errno));
	}

	for (i = 0; i < num_txn_iteration_patterns; i++) {
		free(txn_iteration_patterns[i]);
	}
	for (i = 0; i < num_user_iteration_patterns; i++) {
		free(user_iteration_patterns[i]);
	}
	free(txn_iteration_patterns);
	free(user_iteration_patterns);
}


int main(int argc, char **argv) {
	int rc,
		num_users,
		num_rows_per_user,
		num_rows_per_commit,
		sync_interval_in_seconds,
		mdb_env_flags;
	long long int loaded_rows;
	size_t db_size;
	char *db_dir, *ptr, db_filename_buf[256];
	MDB_env *env;
	MDB_stat mst;
	struct stat st;
	struct timespec load_start, load_end, load_time;
	double load_time_seconds, load_rate;

	errno = 0;
	if (argc != 7) {
		fprintf(stderr, "usage: %s db_dir db_size num_users num_rows_per_user num_rows_per_commit sync_interval_in_seconds\n", argv[0]);
		return EXIT_FAILURE;
	}

	db_dir = argv[1];
	if (parse_human_readable_size(argv[2], &db_size) != 0) {
		return EXIT_FAILURE;
	}

	num_users = strtol(argv[3], &ptr, 0);
	if (errno || num_users <= 0 || *ptr != '\0') {
		fprintf(stderr, "num_users must be a positive integer.\n");
		return EXIT_FAILURE;
	}

	num_rows_per_user = strtol(argv[4], &ptr, 0);
	if (errno || num_rows_per_user <= 0 || *ptr != '\0') {
		fprintf(stderr, "num_rows_per_user must be a positive integer.\n");
		return EXIT_FAILURE;
	}

	num_rows_per_commit = strtol(argv[5], &ptr, 0);
	if (errno || num_rows_per_commit <= 0 || *ptr != '\0') {
		fprintf(stderr, "num_rows_per_commit must be a positive integer.\n");
		return EXIT_FAILURE;
	}

	sync_interval_in_seconds = strtol(argv[6], &ptr, 0);
	if (errno || *ptr != '\0') {
		fprintf(stderr, "sync_interval_in_seconds must be an integer.\n");
		return EXIT_FAILURE;
	}

	mdb_env_flags = MDB_NORDAHEAD;
	if (sync_interval_in_seconds != 0) {
		mdb_env_flags |= MDB_NOSYNC;
	}

	E(mdb_env_create(&env));
	E(mdb_env_set_mapsize(env, db_size));
	E(mdb_env_open(env, db_dir, mdb_env_flags, 0664));

	_insert(env, num_users, num_rows_per_user, num_rows_per_commit, sync_interval_in_seconds, &load_start, &load_end);

	// Compute some stats
	timespec_subtract(&load_time, &load_end, &load_start);
	load_time_seconds = load_time.tv_sec + load_time.tv_nsec / 1e9;
	loaded_rows = num_users * num_rows_per_user;
	load_rate = loaded_rows / load_time_seconds;

	// get file size
	sprintf(db_filename_buf, "%s/data.mdb", db_dir);
	stat(db_filename_buf, &st);

	// get lmdb stats
	E(mdb_env_stat(env, &mst));

	// print everything out
	printf("database stats:\n");
	printf("    page size:      %d\n", mst.ms_psize);
	printf("    tree depth:     %d\n", mst.ms_depth);
	printf("    branch pages:   %zu\n", mst.ms_branch_pages);
	printf("    leaf pages:     %zu\n", mst.ms_leaf_pages);
	printf("    overflow pages: %zu\n", mst.ms_overflow_pages);
	printf("    entries:        %zu\n", mst.ms_entries);
	printf("\n");
	printf("file stats:\n");
	printf("    file size:      %lld\n", st.st_size);
	printf("    avg row size:   %lld\n", st.st_size / loaded_rows);
	printf("\n");
	printf("insert stats:\n");
	printf("    total time:     %.3lfs\n", load_time_seconds);
	printf("    num rows:       %lld\n", loaded_rows);
	printf("    rows/sec:       %.3lf\n", load_rate);
	printf("\n");
	
	mdb_env_close(env);

	return EXIT_SUCCESS;
}
