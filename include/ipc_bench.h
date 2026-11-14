// 새로운 벤치마크에서 사용할 설정과 결과 구조체를 정의한다.

#ifndef IPC_BENCH_H
#define IPC_BENCH_H

#include <stddef.h>
#include <stdint.h>

/* ========================================
   Benchmark Configuration
   ======================================== */

#define MAX_CLIENTS 8

#define MIN_MESSAGE_SIZE 64
#define MAX_MESSAGE_SIZE (1024 * 1024)

/* 클라이언트별 워밍업 요청 수 */
#define DEFAULT_WARMUP_REQUESTS 100

/* 클라이언트별 본 측정 요청 수 */
#define DEFAULT_MEASURE_REQUESTS 1000

/* 실험 반복 횟수 */
#define DEFAULT_REPEAT_COUNT 30

/* ========================================
   IPC Type
   ======================================== */

typedef enum
{
    BENCH_PIPE,
    BENCH_MQ,
    BENCH_SHM
} bench_ipc_type_t;

/* ========================================
   Benchmark Configuration
   ======================================== */

typedef struct
{
    bench_ipc_type_t ipc_type;

    size_t message_size;

    int client_count;

    int warmup_requests;

    int measure_requests;

} ipc_bench_config_t;

/* ========================================
   Resource Usage
   ======================================== */

typedef struct
{
    double user_cpu_sec;
    double system_cpu_sec;

    long voluntary_ctx_switches;
    long involuntary_ctx_switches;

    long peak_rss_kb;

} resource_usage_t;

/* ========================================
   Benchmark Result
   ======================================== */

typedef struct
{
    double elapsed_sec;

    double rtt_mean_us;
    double rtt_median_us;
    double rtt_p99_us;
    double rtt_max_us;

    double messages_per_sec;
    double throughput_mib_s;

    uint64_t completed_requests;

    resource_usage_t resources;

} ipc_bench_result_t;

#endif