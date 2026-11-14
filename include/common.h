// 모든 IPC 실험에서 사용하는 공통 상수와 설정을 정의한다.

#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdint.h>

#define TOTAL_BYTES (256ULL * 1024 * 1024)
#define DEFAULT_CHUNK_SIZE 4096

typedef enum
{
    IPC_PIPE,
    IPC_MSGQ,
    IPC_SHM
} ipc_type_t;

typedef struct
{
    ipc_type_t ipc_type;
    size_t chunk_size;
    size_t total_bytes;
} benchmark_config_t;

#endif