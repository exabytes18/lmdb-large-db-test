#ifndef __UTILS_H
#define __UTILS_H

#include <time.h>

#define MIN(x,y) ((x) < (y) ? (x) : (y))
#define MAX(x,y) ((x) > (y) ? (x) : (y))

int set_nonblocking(int fd);
int force_write(int fd, char b);
int uninterruptable_close(int fd);
void get_nanotime(struct timespec *timespec);
void timespec_normalize(struct timespec *result);
void timespec_add(struct timespec *result, const struct timespec *a, const struct timespec *b);
void timespec_subtract(struct timespec *result, const struct timespec *a, const struct timespec *b);
int parse_human_readable_size(const char *arg, size_t *result);

#endif
