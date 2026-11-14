// 1단계 기본 IPC 기법 중 pipe 방식

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/wait.h>

#include "common.h"
#include "benchmark.h"

/* 요청한 크기만큼 데이터를 모두 전송 */
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

/* 요청한 크기만큼 데이터를 모두 수신 */
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

/* Pipe 성능 측정 */
static int run_pipe_benchmark(size_t chunk_size)
{
    int pipefd[2];
    int resultfd[2];

    /* 데이터 전송용 Pipe */
    if (pipe(pipefd) == -1)
    {
        perror("pipe");
        return -1;
    }

    /* 체크섬 반환용 Pipe */
    if (pipe(resultfd) == -1)
    {
        perror("result pipe");

        close(pipefd[0]);
        close(pipefd[1]);

        return -1;
    }

    pid_t pid = fork();

    if (pid < 0)
    {
        perror("fork");

        close(pipefd[0]);
        close(pipefd[1]);
        close(resultfd[0]);
        close(resultfd[1]);

        return -1;
    }

    /* ========================================
       Child Process: Receiver
       ======================================== */
    if (pid == 0)
    {
        close(pipefd[1]);
        close(resultfd[0]);

        unsigned char *buffer = malloc(chunk_size);

        if (!buffer)
        {
            perror("malloc");

            close(pipefd[0]);
            close(resultfd[1]);

            _exit(EXIT_FAILURE);
        }

        size_t total_received = 0;
        uint64_t checksum = 0;

        while (total_received < TOTAL_BYTES)
        {
            size_t remaining = TOTAL_BYTES - total_received;

            size_t current_size =
                remaining < chunk_size ? remaining : chunk_size;

            if (read_all(pipefd[0], buffer, current_size) < 0)
            {
                perror("read");

                free(buffer);
                close(pipefd[0]);
                close(resultfd[1]);

                _exit(EXIT_FAILURE);
            }

            /* 수신 데이터 체크섬 누적 */
            checksum += calculate_checksum(buffer, current_size);

            total_received += current_size;
        }

        /* 계산한 체크섬을 부모에게 반환 */
        if (write_all(resultfd[1], &checksum, sizeof(checksum)) < 0)
        {
            perror("checksum write");

            free(buffer);
            close(pipefd[0]);
            close(resultfd[1]);

            _exit(EXIT_FAILURE);
        }

        free(buffer);

        close(pipefd[0]);
        close(resultfd[1]);

        _exit(EXIT_SUCCESS);
    }

    /* ========================================
       Parent Process: Sender
       ======================================== */

    close(pipefd[0]);
    close(resultfd[1]);

    unsigned char *buffer = malloc(chunk_size);

    if (!buffer)
    {
        perror("malloc");

        close(pipefd[1]);
        close(resultfd[0]);

        waitpid(pid, NULL, 0);

        return -1;
    }

    /* 전송 데이터 초기화 */
    for (size_t i = 0; i < chunk_size; i++)
    {
        buffer[i] = (unsigned char)(i % 256);
    }

    /* ========================================
       예상 체크섬 계산 (측정 시작 전)
       ======================================== */

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
        close(pipefd[1]);
        close(resultfd[0]);
        waitpid(pid, NULL, 0);
        return -1;
    }

    size_t total_sent = 0;

    while (total_sent < TOTAL_BYTES)
    {
        size_t remaining = TOTAL_BYTES - total_sent;

        size_t current_size =
            remaining < chunk_size ? remaining : chunk_size;

        if (write_all(pipefd[1], buffer, current_size) < 0)
        {
            perror("write");

            free(buffer);
            close(pipefd[1]);
            close(resultfd[0]);

            waitpid(pid, NULL, 0);

            return -1;
        }

        total_sent += current_size;
    }

    /* 전송 완료 */
    close(pipefd[1]);

    /* ========================================
       자식 프로세스 체크섬 수신
       ======================================== */

    uint64_t received_checksum = 0;

    if (read_all(resultfd[0],
                 &received_checksum,
                 sizeof(received_checksum)) < 0)
    {
        perror("checksum read");

        close(resultfd[0]);
        waitpid(pid, NULL, 0);
        free(buffer);

        return -1;
    }

    close(resultfd[0]);

    /* 자식 프로세스 종료 대기 */
    int status;

    while (waitpid(pid, &status, 0) == -1)
    {
        if (errno == EINTR)
            continue;

        perror("waitpid");
        free(buffer);

        return -1;
    }

    double end = get_time_sec();

    free(buffer);

    if (end < 0)
        return -1;

    /* ========================================
       실행 결과 검증
       ======================================== */

    if (!WIFEXITED(status) || WEXITSTATUS(status) != EXIT_SUCCESS)
    {
        fprintf(stderr, "Child process failed\n");
        return -1;
    }

    if (expected_checksum != received_checksum)
    {
        fprintf(stderr,
                "Checksum mismatch!\n"
                "Expected: %llu\n"
                "Received: %llu\n",
                (unsigned long long)expected_checksum,
                (unsigned long long)received_checksum);

        return -1;
    }

    printf("Integrity Check: PASS\n");

    /* ========================================
       성능 결과 출력
       ======================================== */

    benchmark_result_t result = {
        .elapsed_sec = end - start,
        .throughput_mib_s =
            calculate_throughput(TOTAL_BYTES, end - start),
        .checksum = received_checksum};

    print_result("Pipe", chunk_size, TOTAL_BYTES, &result);

    return 0;
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

        if (errno != 0 || *endptr != '\0' ||
            argv[1][0] == '-' || value == 0 ||
            value > TOTAL_BYTES)
        {
            fprintf(stderr, "Invalid chunk size\n");
            return EXIT_FAILURE;
        }

        chunk_size = (size_t)value;
    }

    if (run_pipe_benchmark(chunk_size) < 0)
        return EXIT_FAILURE;

    return EXIT_SUCCESS;
}