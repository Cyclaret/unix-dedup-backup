// 1단계 IPC - Pipe

#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "ipc_bench.h"
#include "bench_metrics.h"

#define WORKERS 3
#define MAX_CHANNELS (MAX_CLIENTS * WORKERS)
#define IO_TIMEOUT_MS 30000
#define RUN_TIMEOUT_SEC 600
#define SHUTDOWN_GRACE_MS 2000

typedef struct
{
    int req[2], resp[2];
} data_channel_t;
typedef struct
{
    int ready[2], go[2], result[2];
} client_control_t;
typedef struct
{
    int start[2], ack[2], result[2];
} server_control_t;
typedef struct
{
    uint64_t completed;
    double finish_sec, user_cpu_sec, sys_cpu_sec;
    long voluntary_cs, involuntary_cs, peak_rss_kb, current_rss_kb;
} report_t;

typedef struct
{
    pthread_mutex_t mutex;
    uint64_t total_requests, total_bytes;
    uint64_t per_client[MAX_CLIENTS];
} shared_stats_t;

typedef struct
{
    int worker_id, clients, warmup, measured;
    size_t size;
    data_channel_t *channels;
    pthread_barrier_t *barrier;
    shared_stats_t *shared;
    int failed;
} server_arg_t;

typedef struct
{
    int client_id, worker_id, warmup, measured;
    size_t size;
    data_channel_t *channel;
    pthread_barrier_t *barrier;
    double *rtts;
    double finish_sec;
    uint64_t completed;
    int failed;
} client_arg_t;

/* Nonblocking descriptors and absolute CLOCK_MONOTONIC deadlines. */
static int set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int create_pipe(int fd[2])
{
    if (pipe(fd) != 0)
        return -1;
    if (set_nonblocking(fd[0]) != 0 || set_nonblocking(fd[1]) != 0)
    {
        close(fd[0]);
        close(fd[1]);
        return -1;
    }
    return 0;
}

static int64_t monotonic_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int wait_ready(int fd, short events, int64_t deadline)
{
    for (;;)
    {
        int64_t now = monotonic_ms();
        if (now < 0)
            return -1;
        int64_t remaining = deadline - now;
        if (remaining <= 0)
        {
            errno = ETIMEDOUT;
            return -1;
        }
        struct pollfd f = {.fd = fd, .events = events};
        int rc = poll(&f, 1, remaining > INT_MAX ? INT_MAX : (int)remaining);
        if (rc < 0 && errno == EINTR)
            continue;
        if (rc == 0)
        {
            errno = ETIMEDOUT;
            return -1;
        }
        if (rc < 0 || (f.revents & (POLLERR | POLLNVAL)))
            return -1;
        if (f.revents & events)
            return 0;
        /* POLLHUP without readable data, or any other terminal event. */
        errno = EPIPE;
        return -1;
    }
}

static int io_all(int fd, void *buffer, size_t size, int writing)
{
    unsigned char *p = buffer;
    int64_t now = monotonic_ms();
    if (now < 0)
        return -1;
    int64_t deadline = now + IO_TIMEOUT_MS;

    while (size > 0)
    {
        ssize_t n = writing ? write(fd, p, size) : read(fd, p, size);
        if (n > 0)
        {
            p += (size_t)n;
            size -= (size_t)n;
            continue;
        }
        if (n == 0)
        {
            errno = EPIPE;
            return -1;
        }
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            return -1;
        if (wait_ready(fd, writing ? POLLOUT : POLLIN, deadline) != 0)
            return -1;
    }
    return 0;
}

static int send_all(int fd, const void *p, size_t n)
{
    return io_all(fd, (void *)p, n, 1);
}

static int recv_all(int fd, void *p, size_t n)
{
    return io_all(fd, p, n, 0);
}

/* A failed worker must terminate its process rather than strand peers at a barrier. */
static void worker_fatal(const char *role, int id)
{
    fprintf(stderr, "%s worker %d failed: %s\n", role, id, strerror(errno));
    _exit(EXIT_FAILURE);
}

static void fill_usage(report_t *r, const struct rusage *before, const struct rusage *after)
{
    bench_resource_t u;
    bench_collect_resources(before, after, &u);
    r->user_cpu_sec = u.user_cpu_sec;
    r->sys_cpu_sec = u.system_cpu_sec;
    r->voluntary_cs = u.voluntary_cs;
    r->involuntary_cs = u.involuntary_cs;
    r->peak_rss_kb = u.peak_rss_kb;
    r->current_rss_kb = u.current_rss_kb;
}

static int server_phase(server_arg_t *a, unsigned char *buf, int count, int measured_phase)
{
    struct pollfd fds[MAX_CLIENTS];
    int done[MAX_CLIENTS] = {0};
    for (int i = 0; i < a->clients; ++i)
    {
        int idx = i * WORKERS + a->worker_id;
        fds[i] = (struct pollfd){.fd = a->channels[idx].req[0], .events = POLLIN};
    }
    uint64_t remaining = (uint64_t)a->clients * (uint64_t)count;
    while (remaining)
    {
        int rc;
        do
        {
            rc = poll(fds, a->clients, IO_TIMEOUT_MS);
        } while (rc < 0 && errno == EINTR);
        if (rc <= 0)
            return -1;
        for (int i = 0; i < a->clients; ++i)
        {
            if (fds[i].fd < 0)
                continue;
            if (!(fds[i].revents & POLLIN))
            {
                if (fds[i].revents & (POLLERR | POLLHUP | POLLNVAL))
                    return -1;
                continue;
            }
            data_channel_t *ch = &a->channels[i * WORKERS + a->worker_id];
            if (recv_all(ch->req[0], buf, a->size) || send_all(ch->resp[1], buf, a->size))
                return -1;
            if (measured_phase)
            {
                /* Real shared state, updated concurrently by all server workers. */
                if (pthread_mutex_lock(&a->shared->mutex) != 0)
                    return -1;
                a->shared->total_requests++;
                a->shared->total_bytes += 2ULL * (uint64_t)a->size;
                a->shared->per_client[i]++;
                pthread_mutex_unlock(&a->shared->mutex);
            }
            ++done[i];
            --remaining;
            if (done[i] == count)
                fds[i].fd = -1;
        }
    }
    return 0;
}

static void *server_worker(void *p)
{
    server_arg_t *a = p;
    unsigned char *buf = malloc(a->size);
    if (!buf)
        worker_fatal("server", a->worker_id);
    if (server_phase(a, buf, a->warmup, 0))
        worker_fatal("server warmup", a->worker_id);
    pthread_barrier_wait(a->barrier);
    if (server_phase(a, buf, a->measured, 1))
        worker_fatal("server measured", a->worker_id);
    free(buf);
    return NULL;
}

static int run_server(data_channel_t *channels, server_control_t *ctrl,
                      int clients, size_t size, int warmup, int measured)
{
    pthread_t threads[WORKERS];
    server_arg_t args[WORKERS];
    pthread_barrier_t barrier;
    shared_stats_t shared = {0};
    if (pthread_mutex_init(&shared.mutex, NULL) != 0)
        return -1;
    if (pthread_barrier_init(&barrier, NULL, WORKERS + 1) != 0)
        return -1;
    for (int i = 0; i < WORKERS; ++i)
    {
        args[i] = (server_arg_t){.worker_id = i, .clients = clients, .warmup = warmup, .measured = measured, .size = size, .channels = channels, .barrier = &barrier, .shared = &shared};
        if (pthread_create(&threads[i], NULL, server_worker, &args[i]) != 0)
            return -1;
    }
    char token;
    if (recv_all(ctrl->start[0], &token, 1) || token != 'S')
        return -1;
    struct rusage before, after;
    if (getrusage(RUSAGE_SELF, &before) != 0)
        return -1;
    token = 'A';
    if (send_all(ctrl->ack[1], &token, 1))
        return -1;
    pthread_barrier_wait(&barrier);
    int failed = 0;
    for (int i = 0; i < WORKERS; ++i)
    {
        pthread_join(threads[i], NULL);
        if (args[i].failed)
            failed = 1;
    }
    if (getrusage(RUSAGE_SELF, &after) != 0)
        failed = 1;
    pthread_barrier_destroy(&barrier);
    if (shared.total_requests != (uint64_t)clients * WORKERS * measured ||
        shared.total_bytes != shared.total_requests * 2ULL * size)
        failed = 1;
    for (int i = 0; i < clients; ++i)
        if (shared.per_client[i] != (uint64_t)WORKERS * measured)
            failed = 1;
    pthread_mutex_destroy(&shared.mutex);
    if (failed)
        return -1;
    report_t r = {.completed = shared.total_requests};
    fill_usage(&r, &before, &after);
    return send_all(ctrl->result[1], &r, sizeof(r));
}

static void *client_worker(void *p)
{
    client_arg_t *a = p;
    unsigned char *send = malloc(a->size), *recv = malloc(a->size);
    if (!send || !recv)
        worker_fatal("client allocation", a->worker_id);
    memset(send, (unsigned char)(a->client_id * WORKERS + a->worker_id + 1), a->size);
    for (int i = 0; i < a->warmup; ++i)
    {
        if (send_all(a->channel->req[1], send, a->size) ||
            recv_all(a->channel->resp[0], recv, a->size) || memcmp(send, recv, a->size))
        {
            worker_fatal("client warmup", a->worker_id);
        }
    }
    pthread_barrier_wait(a->barrier); /* warmup complete */
    pthread_barrier_wait(a->barrier); /* start measured phase */
    for (int i = 0; i < a->measured; ++i)
    {
        double t0 = bench_now_sec();
        if (send_all(a->channel->req[1], send, a->size) ||
            recv_all(a->channel->resp[0], recv, a->size))
        {
            worker_fatal("client measured", a->worker_id);
        }
        double t1 = bench_now_sec();
        if (memcmp(send, recv, a->size))
        {
            worker_fatal("client measured", a->worker_id);
        }
        a->rtts[i] = (t1 - t0) * 1e6;
        ++a->completed;
    }
    a->finish_sec = bench_now_sec();
    free(send);
    free(recv);
    return NULL;
}

static int run_client(int id, data_channel_t *channels, client_control_t *ctrl,
                      size_t size, int warmup, int measured)
{
    pthread_t threads[WORKERS];
    client_arg_t args[WORKERS];
    pthread_barrier_t barrier;
    size_t samples = (size_t)WORKERS * measured;
    double *rtts = malloc(samples * sizeof(double));
    if (!rtts)
        return -1;
    if (pthread_barrier_init(&barrier, NULL, WORKERS + 1) != 0)
    {
        free(rtts);
        return -1;
    }
    for (int i = 0; i < WORKERS; ++i)
    {
        args[i] = (client_arg_t){.client_id = id, .worker_id = i, .warmup = warmup, .measured = measured, .size = size, .channel = &channels[id * WORKERS + i], .barrier = &barrier, .rtts = rtts + (size_t)i * measured};
        if (pthread_create(&threads[i], NULL, client_worker, &args[i]) != 0)
            return -1;
    }
    pthread_barrier_wait(&barrier);
    char token = 'R';
    if (send_all(ctrl->ready[1], &token, 1) || recv_all(ctrl->go[0], &token, 1) || token != 'G')
        return -1;
    struct rusage before, after;
    if (getrusage(RUSAGE_SELF, &before) != 0)
        return -1;
    pthread_barrier_wait(&barrier);
    uint64_t completed = 0;
    double finish = 0;
    int failed = 0;
    for (int i = 0; i < WORKERS; ++i)
    {
        pthread_join(threads[i], NULL);
        completed += args[i].completed;
        if (args[i].finish_sec > finish)
            finish = args[i].finish_sec;
        if (args[i].failed)
            failed = 1;
    }
    if (getrusage(RUSAGE_SELF, &after) != 0)
        failed = 1;
    pthread_barrier_destroy(&barrier);
    if (failed || completed != samples)
    {
        free(rtts);
        return -1;
    }
    report_t r = {.completed = completed, .finish_sec = finish};
    fill_usage(&r, &before, &after);
    int rc = send_all(ctrl->result[1], &r, sizeof(r));
    if (!rc)
        rc = send_all(ctrl->result[1], rtts, samples * sizeof(double));
    free(rtts);
    return rc;
}

static long parse_long(const char *s, long min, long max, const char *name)
{
    char *end;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno || !*s || *end || v < min || v > max)
    {
        fprintf(stderr, "Invalid %s: %s\n", name, s);
        exit(EXIT_FAILURE);
    }
    return v;
}

/* SIGALRM watchdog: also covers barrier deadlocks and stalled waitpid(). */
static volatile sig_atomic_t watchdog_fired = 0;
static volatile sig_atomic_t tracked_server = -1;
static volatile sig_atomic_t tracked_clients[MAX_CLIENTS];

static void watchdog_handler(int signo)
{
    (void)signo;
    watchdog_fired = 1;
    if (tracked_server > 0)
        kill((pid_t)tracked_server, SIGKILL);
    for (int i = 0; i < MAX_CLIENTS; ++i)
        if (tracked_clients[i] > 0)
            kill((pid_t)tracked_clients[i], SIGKILL);
}

static void terminate_children(pid_t server, const pid_t *clients, int count)
{
    if (server > 0)
        kill(server, SIGTERM);
    for (int i = 0; i < count; ++i)
        if (clients[i] > 0)
            kill(clients[i], SIGTERM);
}

static int reap_child(pid_t pid, int *status, int64_t deadline)
{
    for (;;)
    {
        pid_t rc = waitpid(pid, status, WNOHANG);
        if (rc == pid)
            return 0;
        if (rc < 0 && errno == EINTR)
            continue;
        if (rc < 0)
            return -1;
        if (monotonic_ms() >= deadline)
        {
            kill(pid, SIGKILL);
            do
            {
                rc = waitpid(pid, status, 0);
            } while (rc < 0 && errno == EINTR);
            return -1;
        }
        struct timespec pause_time = {.tv_sec = 0, .tv_nsec = 10000000};
        nanosleep(&pause_time, NULL);
    }
}

int main(int argc, char **argv)
{
    if (argc != 4)
    {
        fprintf(stderr, "Usage: %s <message_size> <client_count> <requests_per_worker>\n", argv[0]);
        return EXIT_FAILURE;
    }
    size_t size = (size_t)parse_long(argv[1], MIN_MESSAGE_SIZE, MAX_MESSAGE_SIZE, "message size");
    int clients = (int)parse_long(argv[2], 1, MAX_CLIENTS, "client count");
    int measured = (int)parse_long(argv[3], 1, 100000, "requests per worker");
    int warmup = DEFAULT_WARMUP_REQUESTS;
    int channels_n = clients * WORKERS;
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa = {0};
    sa.sa_handler = watchdog_handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGALRM, &sa, NULL) != 0)
    {
        perror("sigaction");
        return 1;
    }
    alarm(RUN_TIMEOUT_SEC);
    data_channel_t channels[MAX_CHANNELS];
    client_control_t controls[MAX_CLIENTS];
    server_control_t sc;
    for (int i = 0; i < channels_n; ++i)
        if (create_pipe(channels[i].req) || create_pipe(channels[i].resp))
        {
            perror("data pipe");
            return 1;
        }
    for (int i = 0; i < clients; ++i)
        if (create_pipe(controls[i].ready) || create_pipe(controls[i].go) || create_pipe(controls[i].result))
        {
            perror("client control pipe");
            return 1;
        }
    if (create_pipe(sc.start) || create_pipe(sc.ack) || create_pipe(sc.result))
    {
        perror("server control pipe");
        return 1;
    }
    pid_t server = fork();
    if (server < 0)
    {
        perror("fork server");
        return 1;
    }
    if (server == 0)
    {
        for (int i = 0; i < channels_n; ++i)
        {
            close(channels[i].req[1]);
            close(channels[i].resp[0]);
        }
        for (int i = 0; i < clients; ++i)
        {
            close(controls[i].ready[0]);
            close(controls[i].ready[1]);
            close(controls[i].go[0]);
            close(controls[i].go[1]);
            close(controls[i].result[0]);
            close(controls[i].result[1]);
        }
        close(sc.start[1]);
        close(sc.ack[0]);
        close(sc.result[0]);
        _exit(run_server(channels, &sc, clients, size, warmup, measured) ? 1 : 0);
    }
    tracked_server = (sig_atomic_t)server;
    pid_t pids[MAX_CLIENTS] = {0};
    int spawned = 0, failed = 0;
    for (int i = 0; i < clients; ++i)
    {
        pid_t pid = fork();
        if (pid < 0)
        {
            perror("fork client");
            failed = 1;
            break;
        }
        if (pid == 0)
        {
            for (int j = 0; j < channels_n; ++j)
            {
                close(channels[j].req[0]);
                close(channels[j].resp[1]);
                if (j / WORKERS != i)
                {
                    close(channels[j].req[1]);
                    close(channels[j].resp[0]);
                }
            }
            for (int j = 0; j < clients; ++j)
            {
                close(controls[j].ready[0]);
                close(controls[j].go[1]);
                close(controls[j].result[0]);
                if (j != i)
                {
                    close(controls[j].ready[1]);
                    close(controls[j].go[0]);
                    close(controls[j].result[1]);
                }
            }
            close(sc.start[0]);
            close(sc.start[1]);
            close(sc.ack[0]);
            close(sc.ack[1]);
            close(sc.result[0]);
            close(sc.result[1]);
            _exit(run_client(i, channels, &controls[i], size, warmup, measured) ? 1 : 0);
        }
        pids[spawned] = pid;
        tracked_clients[spawned] = (sig_atomic_t)pid;
        spawned++;
    }
    for (int i = 0; i < channels_n; ++i)
    {
        close(channels[i].req[0]);
        close(channels[i].req[1]);
        close(channels[i].resp[0]);
        close(channels[i].resp[1]);
    }
    for (int i = 0; i < clients; ++i)
    {
        close(controls[i].ready[1]);
        close(controls[i].go[0]);
        close(controls[i].result[1]);
    }
    close(sc.start[0]);
    close(sc.ack[1]);
    close(sc.result[1]);
    double *rtts = NULL;
    if (failed)
        goto cleanup;
    for (int i = 0; i < clients; ++i)
    {
        char token;
        if (recv_all(controls[i].ready[0], &token, 1) || token != 'R')
            failed = 1;
        close(controls[i].ready[0]);
        if (failed)
            goto cleanup;
    }
    char token = 'S';
    if (send_all(sc.start[1], &token, 1))
    {
        failed = 1;
        goto cleanup;
    }
    close(sc.start[1]);
    if (recv_all(sc.ack[0], &token, 1) || token != 'A')
    {
        failed = 1;
        goto cleanup;
    }
    close(sc.ack[0]);
    double start = bench_now_sec();
    for (int i = 0; i < clients; ++i)
    {
        token = 'G';
        if (send_all(controls[i].go[1], &token, 1))
            failed = 1;
        close(controls[i].go[1]);
    }
    if (failed)
        goto cleanup;
    size_t samples_per_client = (size_t)WORKERS * measured;
    size_t total_samples = (size_t)clients * samples_per_client;
    rtts = malloc(total_samples * sizeof(double));
    if (!rtts)
    {
        failed = 1;
        goto cleanup;
    }
    uint64_t completed = 0;
    double finish = start, cu = 0, cs = 0;
    long cv = 0, ci = 0, cp = 0, cr = 0;
    for (int i = 0; i < clients; ++i)
    {
        report_t r;
        if (recv_all(controls[i].result[0], &r, sizeof(r)) ||
            r.completed != samples_per_client ||
            recv_all(controls[i].result[0], rtts + (size_t)i * samples_per_client,
                     samples_per_client * sizeof(double)))
        {
            failed = 1;
            goto cleanup;
        }
        close(controls[i].result[0]);
        completed += r.completed;
        if (r.finish_sec > finish)
            finish = r.finish_sec;
        cu += r.user_cpu_sec;
        cs += r.sys_cpu_sec;
        cv += r.voluntary_cs;
        ci += r.involuntary_cs;
        cp += r.peak_rss_kb;
        if (r.current_rss_kb >= 0)
            cr += r.current_rss_kb;
    }
    report_t sr;
    if (recv_all(sc.result[0], &sr, sizeof(sr)) || sr.completed != total_samples)
    {
        failed = 1;
        goto cleanup;
    }
    close(sc.result[0]);
    bench_rtt_stats_t stats;
    bench_calculate_rtt(rtts, total_samples, &stats);
    double elapsed = finish - start;
    if (elapsed <= 0)
    {
        failed = 1;
        goto cleanup;
    }
    printf("IPC: Pipe\nMessage Size: %zu\nClients: %d\nWorkers per Process: %d\n", size, clients, WORKERS);
    printf("Warmup Requests per Worker: %d\nMeasure Requests per Worker: %d\n", warmup, measured);
    printf("Completed Requests: %" PRIu64 "\nElapsed Time: %.6f sec\n", completed, elapsed);
    printf("RTT Mean: %.3f us\nRTT Median: %.3f us\nRTT P99: %.3f us\nRTT Max: %.3f us\n",
           stats.mean_us, stats.median_us, stats.p99_us, stats.max_us);
    printf("Messages/sec: %.2f\nThroughput: %.2f MiB/s\n", completed / elapsed,
           2.0 * size * completed / (1024.0 * 1024.0 * elapsed));
    printf("\n[Client Resources]\nCPU User: %.6f sec\nCPU System: %.6f sec\n", cu, cs);
    printf("Voluntary CS: %ld\nInvoluntary CS: %ld\nPeak RSS Sum: %ld kB\nCurrent RSS Sum: %ld kB\n", cv, ci, cp, cr);
    printf("\n[Server Resources]\nCPU User: %.6f sec\nCPU System: %.6f sec\n", sr.user_cpu_sec, sr.sys_cpu_sec);
    printf("Voluntary CS: %ld\nInvoluntary CS: %ld\nPeak RSS: %ld kB\nCurrent RSS: %ld kB\n",
           sr.voluntary_cs, sr.involuntary_cs, sr.peak_rss_kb, sr.current_rss_kb);
    printf("Shared Statistics: PASS\nIntegrity Check: PASS\n");
cleanup:
    if (failed || watchdog_fired)
        terminate_children(server, pids, spawned);
    int64_t shutdown_deadline = monotonic_ms() + SHUTDOWN_GRACE_MS;
    for (int i = 0; i < spawned; ++i)
    {
        int status = 0;
        if (reap_child(pids[i], &status, shutdown_deadline) != 0 ||
            !WIFEXITED(status) || WEXITSTATUS(status) != 0)
            failed = 1;
        tracked_clients[i] = -1;
    }
    int status = 0;
    if (reap_child(server, &status, shutdown_deadline) != 0 ||
        !WIFEXITED(status) || WEXITSTATUS(status) != 0)
        failed = 1;
    tracked_server = -1;
    alarm(0);
    if (watchdog_fired)
        failed = 1;
    free(rtts);
    if (failed)
        fprintf(stderr, "Benchmark failed (IPC timeout, worker failure, or child error)\n");
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
