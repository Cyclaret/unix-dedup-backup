#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "bench_metrics.h"

/* ==========================================
   Time
   ========================================== */

double bench_now_sec(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }

    return (double)ts.tv_sec +
           (double)ts.tv_nsec / 1e9;
}

/* ==========================================
   Resource Usage
   ========================================== */

static double timeval_to_sec(struct timeval t)
{
    return (double)t.tv_sec +
           (double)t.tv_usec / 1e6;
}

static long current_rss_kb(void)
{
    FILE *f = fopen("/proc/self/status", "r");

    if (!f)
        return -1;

    char line[256];
    long kb = -1;

    while (fgets(line, sizeof(line), f)) {
        if (sscanf(line, "VmRSS: %ld kB", &kb) == 1)
            break;
    }

    fclose(f);

    return kb;
}

void bench_collect_resources(
    const struct rusage *before,
    const struct rusage *after,
    bench_resource_t *result
)
{
    result->user_cpu_sec =
        timeval_to_sec(after->ru_utime) -
        timeval_to_sec(before->ru_utime);

    result->system_cpu_sec =
        timeval_to_sec(after->ru_stime) -
        timeval_to_sec(before->ru_stime);

    result->voluntary_cs =
        after->ru_nvcsw - before->ru_nvcsw;

    result->involuntary_cs =
        after->ru_nivcsw - before->ru_nivcsw;

    result->peak_rss_kb = after->ru_maxrss;

    result->current_rss_kb = current_rss_kb();
}

/* ==========================================
   RTT Statistics
   ========================================== */

static int compare_double(const void *a, const void *b)
{
    double x = *(const double *)a;
    double y = *(const double *)b;

    return (x > y) - (x < y);
}

static double percentile(
    const double *values,
    size_t count,
    double p
)
{
    double position = p * (double)(count - 1);

    size_t low = (size_t)position;
    size_t high = low + 1 < count ? low + 1 : low;

    return values[low] +
           (values[high] - values[low]) *
           (position - (double)low);
}

void bench_calculate_rtt(
    double *samples,
    size_t count,
    bench_rtt_stats_t *result
)
{
    if (!samples || !result || count == 0)
        return;

    qsort(
        samples,
        count,
        sizeof(double),
        compare_double
    );

    double sum = 0.0;

    for (size_t i = 0; i < count; i++)
        sum += samples[i];

    result->mean_us = sum / (double)count;
    result->median_us = percentile(samples, count, 0.50);
    result->p99_us = percentile(samples, count, 0.99);
    result->max_us = samples[count - 1];
}