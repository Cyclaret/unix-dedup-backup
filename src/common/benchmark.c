// 실제 측정 함수

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdint.h>
#include <time.h>

#include "benchmark.h"

double get_time_sec(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1)
    {
        perror("clock_gettime");
        return -1.0;
    }

    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

double calculate_throughput(size_t total_bytes, double elapsed_sec)
{
    if (elapsed_sec <= 0.0)
    {
        return 0.0;
    }

    double total_mib = (double)total_bytes / (1024.0 * 1024.0);

    return total_mib / elapsed_sec;
}

uint64_t calculate_checksum(const void *data, size_t size)
{
    const unsigned char *bytes = data;

    uint64_t hash = 14695981039346656037ULL;

    for (size_t i = 0; i < size; i++)
    {
        hash ^= bytes[i];
        hash *= 1099511628211ULL;
    }

    return hash;
}

void print_result(
    const char *ipc_name,
    size_t chunk_size,
    size_t total_bytes,
    const benchmark_result_t *result)
{
    printf("IPC: %s\n", ipc_name);
    printf("Chunk Size: %zu bytes\n", chunk_size);
    printf("Total Bytes: %zu bytes\n", total_bytes);
    printf("Elapsed Time: %.6f sec\n", result->elapsed_sec);
    printf("Throughput: %.2f MiB/s\n", result->throughput_mib_s);
    printf("Checksum: %llu\n",
           (unsigned long long)result->checksum);
}