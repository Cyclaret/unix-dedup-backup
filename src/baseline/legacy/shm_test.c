// 1단계 기본 IPC 기법 중 SHM 방식

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/shm.h>
#include <sys/sem.h>

#include "common.h"
#include "benchmark.h"

#define SEM_EMPTY 0
#define SEM_FULL 1
#define SEM_COUNT 2

/* Linux의 semctl() 초기화에 사용하는 union */
union semun
{
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

/* 공유 메모리 구조체: 실제 데이터 영역은 가변 길이 */
typedef struct
{
    size_t data_size;
    unsigned char data[];
} shared_buffer_t;

/* 세마포어 P/V 연산 */
static int semaphore_op(int semid, unsigned short sem_num, short operation)
{
    struct sembuf op = {
        .sem_num = sem_num,
        .sem_op = operation,
        .sem_flg = 0};

    while (semop(semid, &op, 1) == -1)
    {
        if (errno == EINTR)
            continue;

        return -1;
    }

    return 0;
}

/* 결과 반환용 Pipe */
static int write_all(int fd, const void *buf, size_t size)
{
    const unsigned char *p = buf;
    size_t written = 0;

    while (written < size)
    {
        ssize_t n = write(fd, p + written, size - written);

        if (n < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        if (n == 0)
            return -1;

        written += (size_t)n;
    }

    return 0;
}

static int read_all(int fd, void *buf, size_t size)
{
    unsigned char *p = buf;
    size_t received = 0;

    while (received < size)
    {
        ssize_t n = read(fd, p + received, size - received);

        if (n < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        if (n == 0)
            return -1;

        received += (size_t)n;
    }

    return 0;
}

/* Shared Memory 성능 측정 */
static int run_shm_benchmark(size_t chunk_size)
{
    int shmid = -1;
    int semid = -1;
    int resultfd[2] = {-1, -1};

    shared_buffer_t *shared = NULL;

    /*
     * 1. 공유 메모리 생성
     *
     * 헤더 + 청크 크기만큼 공간 확보
     */
    size_t shm_size = sizeof(shared_buffer_t) + chunk_size;

    shmid = shmget(IPC_PRIVATE, shm_size, IPC_CREAT | 0600);

    if (shmid == -1)
    {
        perror("shmget");
        return -1;
    }

    shared = shmat(shmid, NULL, 0);

    if (shared == (void *)-1)
    {
        perror("shmat");
        shmctl(shmid, IPC_RMID, NULL);
        return -1;
    }

    shared->data_size = 0;

    /*
     * 2. 세마포어 생성 및 초기화
     */
    semid = semget(IPC_PRIVATE, SEM_COUNT, IPC_CREAT | 0600);

    if (semid == -1)
    {
        perror("semget");
        shmdt(shared);
        shmctl(shmid, IPC_RMID, NULL);
        return -1;
    }

    union semun arg;

    arg.val = 1;

    if (semctl(semid, SEM_EMPTY, SETVAL, arg) == -1)
    {
        perror("semctl EMPTY");
        goto cleanup;
    }

    arg.val = 0;

    if (semctl(semid, SEM_FULL, SETVAL, arg) == -1)
    {
        perror("semctl FULL");
        goto cleanup;
    }

    /*
     * 3. 체크섬 반환용 Pipe
     */
    if (pipe(resultfd) == -1)
    {
        perror("pipe");
        goto cleanup;
    }

    pid_t pid = fork();

    if (pid < 0)
    {
        perror("fork");
        goto cleanup;
    }

    /* ========================================
       Child Process: Receiver
       ======================================== */
    if (pid == 0)
    {
        close(resultfd[0]);

        unsigned char *buffer = malloc(chunk_size);

        if (!buffer)
        {
            perror("malloc");
            goto child_error;
        }

        size_t total_received = 0;
        uint64_t checksum = 0;

        while (total_received < TOTAL_BYTES)
        {
            /* 데이터가 준비될 때까지 대기 */
            if (semaphore_op(semid, SEM_FULL, -1) < 0)
            {
                perror("P(FULL)");
                free(buffer);
                goto child_error;
            }

            size_t current_size = shared->data_size;

            if (current_size == 0 ||
                current_size > chunk_size ||
                current_size > TOTAL_BYTES - total_received)
            {
                fprintf(stderr, "Invalid shared data size\n");
                free(buffer);
                goto child_error;
            }

            /*
             * 공유 메모리에서 로컬 버퍼로 복사
             *
             * Pipe/MQ와 마찬가지로 수신 데이터에 대한
             * 실제 읽기 작업을 수행한다.
             */
            memcpy(buffer, shared->data, current_size);

            /*
             * 복사가 끝났으므로 부모에게
             * 다음 데이터를 기록해도 된다고 알린다.
             */
            if (semaphore_op(semid, SEM_EMPTY, 1) < 0)
            {
                perror("V(EMPTY)");
                free(buffer);
                goto child_error;
            }

            checksum += calculate_checksum(buffer, current_size);

            total_received += current_size;
        }

        if (write_all(resultfd[1], &checksum, sizeof(checksum)) < 0)
        {
            perror("checksum write");
            free(buffer);
            goto child_error;
        }

        free(buffer);

        shmdt(shared);
        close(resultfd[1]);

        _exit(EXIT_SUCCESS);

    child_error:
        shmdt(shared);
        close(resultfd[1]);
        _exit(EXIT_FAILURE);
    }

    /* ========================================
       Parent Process: Sender
       ======================================== */

    close(resultfd[1]);
    resultfd[1] = -1;

    unsigned char *buffer = malloc(chunk_size);

    if (!buffer)
    {
        perror("malloc");
        goto parent_error;
    }

    /* 전송 데이터 초기화 */
    for (size_t i = 0; i < chunk_size; i++)
    {
        buffer[i] = (unsigned char)(i % 256);
    }

    /* 예상 체크섬 계산 */
    uint64_t expected_checksum = 0;
    size_t remaining = TOTAL_BYTES;

    while (remaining > 0)
    {
        size_t current_size =
            remaining < chunk_size ? remaining : chunk_size;

        expected_checksum += calculate_checksum(buffer, current_size);

        remaining -= current_size;
    }

    /* ========================================
       성능 측정 시작
       ======================================== */

    double start = get_time_sec();

    if (start < 0)
    {
        free(buffer);
        goto parent_error;
    }

    size_t total_sent = 0;

    while (total_sent < TOTAL_BYTES)
    {
        size_t remaining = TOTAL_BYTES - total_sent;

        size_t current_size =
            remaining < chunk_size ? remaining : chunk_size;

        /* 공유 메모리가 비어 있을 때까지 대기 */
        if (semaphore_op(semid, SEM_EMPTY, -1) < 0)
        {
            perror("P(EMPTY)");
            free(buffer);
            goto parent_error;
        }

        memcpy(shared->data, buffer, current_size);
        shared->data_size = current_size;

        /* 자식에게 데이터 준비 알림 */
        if (semaphore_op(semid, SEM_FULL, 1) < 0)
        {
            perror("V(FULL)");
            free(buffer);
            goto parent_error;
        }

        total_sent += current_size;
    }

    free(buffer);

    /* ========================================
       체크섬 수신
       ======================================== */

    uint64_t received_checksum = 0;

    if (read_all(resultfd[0],
                 &received_checksum,
                 sizeof(received_checksum)) < 0)
    {
        perror("checksum read");
        goto parent_error;
    }

    close(resultfd[0]);
    resultfd[0] = -1;

    int status;

    while (waitpid(pid, &status, 0) == -1)
    {
        if (errno == EINTR)
            continue;

        perror("waitpid");
        goto cleanup;
    }

    double end = get_time_sec();

    if (end < 0)
        goto cleanup;

    /* ========================================
       결과 검증
       ======================================== */

    if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS)
    {
        fprintf(stderr, "Child process failed\n");
        goto cleanup;
    }

    if (expected_checksum != received_checksum)
    {
        fprintf(stderr,
                "Checksum mismatch!\n"
                "Expected: %llu\n"
                "Received: %llu\n",
                (unsigned long long)expected_checksum,
                (unsigned long long)received_checksum);

        goto cleanup;
    }

    printf("Integrity Check: PASS\n");

    benchmark_result_t result = {
        .elapsed_sec = end - start,
        .throughput_mib_s =
            calculate_throughput(TOTAL_BYTES, end - start),
        .checksum = received_checksum};

    print_result("Shared Memory", chunk_size, TOTAL_BYTES, &result);

    /* 정상 종료 및 자원 제거 */
    shmdt(shared);
    shmctl(shmid, IPC_RMID, NULL);
    semctl(semid, 0, IPC_RMID);

    return 0;

parent_error:
    /*
     * 자식이 세마포어에서 대기 중일 수 있으므로
     * 종료시킨 뒤 자원을 정리한다.
     */
    kill(pid, SIGTERM);

    while (waitpid(pid, NULL, 0) == -1 && errno == EINTR)
        ;

cleanup:
    if (resultfd[0] != -1)
        close(resultfd[0]);

    if (resultfd[1] != -1)
        close(resultfd[1]);

    if (shared != NULL && shared != (void *)-1)
        shmdt(shared);

    if (shmid != -1)
        shmctl(shmid, IPC_RMID, NULL);

    if (semid != -1)
        semctl(semid, 0, IPC_RMID);

    return -1;
}

/* ========================================
   Main
   ======================================== */

int main(int argc, char *argv[])
{
    size_t chunk_size = DEFAULT_CHUNK_SIZE;

    if (argc > 1)
    {
        char *endptr;
        errno = 0;

        unsigned long long value =
            strtoull(argv[1], &endptr, 10);

        if (errno != 0 ||
            *endptr != '\0' ||
            argv[1][0] == '-' ||
            value == 0 ||
            value > TOTAL_BYTES)
        {
            fprintf(stderr, "Invalid chunk size\n");
            return EXIT_FAILURE;
        }

        chunk_size = (size_t)value;
    }

    if (run_shm_benchmark(chunk_size) < 0)
        return EXIT_FAILURE;

    return EXIT_SUCCESS;
}