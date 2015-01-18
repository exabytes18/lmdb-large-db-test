#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "utils.h"

#define SECONDS_TO_NANOSECONDS(seconds) (seconds * 1000000000);


int set_nonblocking(int fd) {
	int flags, err;
	for (;;) {
		flags = fcntl(fd, F_GETFL);
		if (flags == -1) {
			if (errno == EINTR) {
				continue;
			}
			return flags;
		}
		
		err = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
		if (err == -1 && errno == EINTR) {
			continue;
		}
		return err;
	}
}


int force_write(int fd, char b) {
	ssize_t bytes_written;
	for (;;) {
		bytes_written = write(fd, &b, 1);
		if (bytes_written == 0) {
			continue;
		}
		if (bytes_written == -1) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
		}
		return bytes_written;
	}
}


int uninterruptable_close(int fd) {
	int err;
	for (;;) {
		err = close(fd);
		if (err == -1 && errno == EINTR) {
			continue;
		}
		return err;
	}
}


#ifdef __APPLE__
#include <mach/mach_time.h>

static mach_timebase_info_data_t _TIMEBASE;
static pthread_once_t _TIMEBASE_INIT = PTHREAD_ONCE_INIT;

static void _timebase_init(void) {
	if (mach_timebase_info(&_TIMEBASE) != KERN_SUCCESS) {
		fprintf(stderr, "CRITICAL! mach_timebase_info() failed\n");
		abort();
	}
}

void get_nanotime(struct timespec *timespec) {
	uint64_t t;

	pthread_once(&_TIMEBASE_INIT, _timebase_init);
	t = (mach_absolute_time() * _TIMEBASE.numer) / _TIMEBASE.denom;
	timespec->tv_sec = t / SECONDS_TO_NANOSECONDS(1);
	timespec->tv_nsec = t - SECONDS_TO_NANOSECONDS(timespec->tv_sec);
}
#else
void get_nanotime(struct timespec *timespec) {
	if (clock_gettime(CLOCK_MONOTONIC, timespec) != 0) {
		fprintf(stderr, "CRITICAL! clock_gettime(): %s\n", strerror(errno));
		abort();
	}
}
#endif


inline void timespec_normalize(struct timespec *result) {
	long overflow_seconds;

	overflow_seconds = result->tv_nsec / SECONDS_TO_NANOSECONDS(1);
	result->tv_sec += overflow_seconds;
	result->tv_nsec -= SECONDS_TO_NANOSECONDS(overflow_seconds);

	if (result->tv_sec > 0 && result->tv_nsec < 0) {
		result->tv_sec -= 1;
		result->tv_nsec += SECONDS_TO_NANOSECONDS(1);
	}

	if (result->tv_sec < 0 && result->tv_nsec > 0) {
		result->tv_sec += 1;
		result->tv_nsec -= SECONDS_TO_NANOSECONDS(1);
	}
}


void timespec_add(struct timespec *result, const struct timespec *a, const struct timespec *b) {
	result->tv_sec = a->tv_sec + b->tv_sec;
	result->tv_nsec = a->tv_nsec + b->tv_nsec;
	timespec_normalize(result);
}


void timespec_subtract(struct timespec *result, const struct timespec *a, const struct timespec *b) {
	result->tv_sec = a->tv_sec - b->tv_sec;
	result->tv_nsec = a->tv_nsec - b->tv_nsec;
	timespec_normalize(result);
}


static int _parse_multiplier(const char *arg, size_t *result) {
	const char *str = arg;
	while(str != '\0' && isspace(*str)) {
		str++;
	}

	if(strcasecmp(str, "") == 0 || strcasecmp(str, "b") == 0) {
		*result = 1ULL;
		return 0;
	} else if(strcasecmp(str, "k") == 0 || strcasecmp(str, "kb") == 0) {
		*result = 1ULL << 10;
		return 0;
	} else if(strcasecmp(str, "m") == 0 || strcasecmp(str, "mb") == 0) {
		*result = 1ULL << 20;
		return 0;
	} else if(strcasecmp(str, "g") == 0 || strcasecmp(str, "gb") == 0) {
		*result = 1ULL << 30;
		return 0;
	} else if(strcasecmp(str, "t") == 0 || strcasecmp(str, "tb") == 0) {
		*result = 1ULL << 40;
		return 0;
	} else if(strcasecmp(str, "p") == 0 || strcasecmp(str, "pb") == 0) {
		*result = 1ULL << 40;
		return 0;
	} else if(strcasecmp(str, "e") == 0 || strcasecmp(str, "eb") == 0) {
		*result = 1ULL << 50;
		return 0;
	} else if(strcasecmp(str, "z") == 0 || strcasecmp(str, "zb") == 0) {
		fprintf(stderr, "zettabytes, really?\n");
		return 1;
	} else if(strcasecmp(str, "y") == 0 || strcasecmp(str, "yb") == 0) {
		fprintf(stderr, "yottabytes, impossible.\n");
		return 1;
	} else {
		fprintf(stderr, "unknown unit\n");
		return 1;
	}
}


int parse_human_readable_size(const char *arg, size_t *result) {
	char *ptr = NULL;
	errno = 0;

	ssize_t first_num = strtoll(arg, &ptr, 10);
	if(errno != 0 || arg == ptr) {
		fprintf(stderr, "problem parsing argument: %s\n", arg);
		return 1;
	}

	size_t multiplier;
	if(_parse_multiplier(ptr, &multiplier) != 0) {
		fprintf(stderr, "problem parsing argument: %s\n", arg);
		return 1;
	}

	// We /could/ overflow here, but it really isn't worth checking.
	ssize_t s = first_num * multiplier;
	if(s <= 0) {
		fprintf(stderr, "invalid argument: %s\n", arg);
		return 1;
	}

	*result = s;
	return 0;
}
