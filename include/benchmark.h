// 성능 측정에 사용할 함수와 결과 구조체를 선언한다.

#ifndef BENCHMARK_H
#define BENCHMARK_H

#include <stddef.h>
#include <stdint.h>

typedef struct
{
    double elapsed_sec;
    double throughput_mib_s;
    uint64_t checksum;
} benchmark_result_t;

// 현재 시간을 측정한다.
double get_time_sec(void);

// 초당 처리량을 계산한다.
double calculate_throughput(size_t total_bytes, double elapsed_sec);

// 데이터 무결성을 검증한다.
uint64_t calculate_checksum(const void *data, size_t size);

// 실험 결과를 출력한다.
void print_result(
    const char *ipc_name,
    size_t chunk_size,
    size_t total_bytes,
    const benchmark_result_t *result);

#endif