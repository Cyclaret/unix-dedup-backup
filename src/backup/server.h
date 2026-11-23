#pragma once
#include <pthread.h>
#include "backup.h"
#include <sys/resource.h>
typedef struct {
    uint64_t first, last, lock_ns, unique, stored;
    long rss;
    struct rusage usage;
} Stats;
extern Stats *stats;
extern size_t count, file_size; /* chunks per client, bytes per file */
extern int striped, duplicate_percent;
extern unsigned char *source;
extern const char *restore_dir;
uint64_t now_ns(void);
void usage_snapshot(Stats *s);
void *allocate(size_t n);
void thread_start(pthread_t *t, void *(*fn)(void *), long id);
void server(void);
