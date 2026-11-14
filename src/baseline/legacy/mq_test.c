// 1단계 기본 IPC 기법 중 message queue 방식

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/msg.h>

#include "common.h"
#include "benchmark.h"

/* Ubuntu 환경의 System V MQ 메시지 최대 크기 */
#define MQ_DATA_SIZE 8192

typedef struct
{
    long mtype;
    unsigned char data[MQ_DATA_SIZE];
} mq_message_t;

/* 체크섬 반환용 Pipe 함수 */
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

/* 메시지 전송 */
static int send_message(int msgid, mq_message_t *msg, size_t size)
{
    while (msgsnd(msgid, msg, size, 0) == -1)
    {
        if (errno == EINTR)
            continue;

        return -1;
    }

    return 0;
}

/* 메시지 수신 */
static int receive_message(int msgid, mq_message_t *msg, size_t size)
{
    while (1)
    {
        ssize_t n = msgrcv(msgid, msg, MQ_DATA_SIZE, 1, 0);

        if (n < 0)
        {
            if (errno == EINTR)
                continue;

            return -1;
        }

        if ((size_t)n != size)
        {
            fprintf(stderr, "Unexpected message size\n");
            return -1;
        }

        return 0;
    }
}

/* Message Queue 성능 측정 */
static int run_mq_benchmark(size_t chunk_size)
{
    /* System V Message Queue 생성 */
    int msgid = msgget(IPC_PRIVATE, IPC_CREAT | 0600);

    if (msgid == -1)
    {
        perror("msgget");
        return -1;
    }

    /* 체크섬 반환용 Pipe */
    int resultfd[2];

    if (pipe(resultfd) == -1)
    {
        perror("pipe");
        msgctl(msgid, IPC_RMID, NULL);
        return -1;
    }

    pid_t pid = fork();

    if (pid < 0)
    {
        perror("fork");

        close(resultfd[0]);
        close(resultfd[1]);

        msgctl(msgid, IPC_RMID, NULL);

        return -1;
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
            close(resultfd[1]);
            _exit(EXIT_FAILURE);
        }

        mq_message_t msg;

        size_t total_received = 0;
        uint64_t checksum = 0;

        while (total_received < TOTAL_BYTES)
        {
            size_t remaining = TOTAL_BYTES - total_received;

            size_t current_chunk =
                remaining < chunk_size ? remaining : chunk_size;

            size_t chunk_received = 0;

            /* 논리적 청크를 여러 메시지로 수신 */
            while (chunk_received < current_chunk)
            {
                size_t fragment_size =
                    current_chunk - chunk_received;

                if (fragment_size > MQ_DATA_SIZE)
                    fragment_size = MQ_DATA_SIZE;

                if (receive_message(msgid, &msg, fragment_size) < 0)
                {
                    perror("msgrcv");

                    free(buffer);
                    close(resultfd[1]);

                    _exit(EXIT_FAILURE);
                }

                memcpy(
                    buffer + chunk_received,
                    msg.data,
                    fragment_size);

                chunk_received += fragment_size;
            }

            checksum += calculate_checksum(buffer, current_chunk);

            total_received += current_chunk;
        }

        /* 체크섬 반환 */
        if (write_all(resultfd[1], &checksum, sizeof(checksum)) < 0)
        {
            perror("checksum write");

            free(buffer);
            close(resultfd[1]);

            _exit(EXIT_FAILURE);
        }

        free(buffer);
        close(resultfd[1]);

        _exit(EXIT_SUCCESS);
    }

    /* ========================================
       Parent Process: Sender
       ======================================== */

    close(resultfd[1]);

    unsigned char *buffer = malloc(chunk_size);

    if (!buffer)
    {
        perror("malloc");

        close(resultfd[0]);
        msgctl(msgid, IPC_RMID, NULL);
        waitpid(pid, NULL, 0);

        return -1;
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
        size_t current_chunk =
            remaining < chunk_size ? remaining : chunk_size;

        expected_checksum += calculate_checksum(buffer, current_chunk);

        remaining -= current_chunk;
    }

    mq_message_t msg;
    msg.mtype = 1;

    /* ========================================
       성능 측정 시작
       ======================================== */

    double start = get_time_sec();

    if (start < 0)
    {
        free(buffer);
        close(resultfd[0]);
        msgctl(msgid, IPC_RMID, NULL);
        waitpid(pid, NULL, 0);
        return -1;
    }

    size_t total_sent = 0;

    while (total_sent < TOTAL_BYTES)
    {
        size_t remaining = TOTAL_BYTES - total_sent;

        size_t current_chunk =
            remaining < chunk_size ? remaining : chunk_size;

        size_t chunk_sent = 0;

        /* 논리적 청크를 여러 메시지로 분할 */
        while (chunk_sent < current_chunk)
        {
            size_t fragment_size =
                current_chunk - chunk_sent;

            if (fragment_size > MQ_DATA_SIZE)
                fragment_size = MQ_DATA_SIZE;

            memcpy(
                msg.data,
                buffer + chunk_sent,
                fragment_size);

            if (send_message(msgid, &msg, fragment_size) < 0)
            {
                perror("msgsnd");

                free(buffer);
                close(resultfd[0]);

                msgctl(msgid, IPC_RMID, NULL);
                waitpid(pid, NULL, 0);

                return -1;
            }

            chunk_sent += fragment_size;
        }

        total_sent += current_chunk;
    }

    /* ========================================
       체크섬 수신
       ======================================== */

    uint64_t received_checksum = 0;

    if (read_all(
            resultfd[0],
            &received_checksum,
            sizeof(received_checksum)) < 0)
    {
        perror("checksum read");

        free(buffer);
        close(resultfd[0]);

        msgctl(msgid, IPC_RMID, NULL);
        waitpid(pid, NULL, 0);

        return -1;
    }

    close(resultfd[0]);

    /* 자식 종료 대기 */
    int status;

    while (waitpid(pid, &status, 0) == -1)
    {
        if (errno == EINTR)
            continue;

        perror("waitpid");

        free(buffer);
        msgctl(msgid, IPC_RMID, NULL);

        return -1;
    }

    double end = get_time_sec();

    free(buffer);

    /* Message Queue 제거 */
    if (msgctl(msgid, IPC_RMID, NULL) == -1)
    {
        perror("msgctl");
        return -1;
    }

    if (end < 0)
        return -1;

    /* ========================================
       결과 검증
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

    benchmark_result_t result = {
        .elapsed_sec = end - start,
        .throughput_mib_s =
            calculate_throughput(TOTAL_BYTES, end - start),
        .checksum = received_checksum};

    print_result("Message Queue", chunk_size, TOTAL_BYTES, &result);

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

    if (run_mq_benchmark(chunk_size) < 0)
        return EXIT_FAILURE;

    return EXIT_SUCCESS;
}