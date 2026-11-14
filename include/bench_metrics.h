// 공통 측정 함수 선언

#ifndef BENCH_METRICS_H
#define BENCH_METRICS_H

#include <stddef.h>
#include <sys/resource.h>

typedef struct
{
    double user_cpu_sec;
    double system_cpu_sec;

    long voluntary_cs;
    long involuntary_cs;

    long peak_rss_kb;
    long current_rss_kb;
} bench_resource_t;

typedef struct
{
    double mean_us;
    double median_us;
    double p99_us;
    double max_us;
} bench_rtt_stats_t;

/* 현재 시각: CLOCK_MONOTONIC */
double bench_now_sec(void);

/* CPU, Context Switch, RSS 측정 */
void bench_collect_resources(
    const struct rusage *before,
    const struct rusage *after,
    bench_resource_t *result);

/* RTT 통계 계산: samples 배열을 정렬함 */
void bench_calculate_rtt(
    double *samples,
    size_t count,
    bench_rtt_stats_t *result);

#endif