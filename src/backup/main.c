#define _GNU_SOURCE
#include "server.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
int lanes, chunk_size, striped, duplicate_percent;
size_t count, file_size;
unsigned char *source;
const char *restore_dir;
Stats *stats;
pthread_barrier_t *start_gate;
static uint64_t *latency;
static pid_t owner, children[9];
static int child_count;
void fail(const char *s) {
    perror(s);
    if (getpid() == owner)
        exit(1);
    _exit(1);
}
void *allocate(size_t n) {
    void *p = malloc(n);
    if (!p)
        fail("malloc");
    return p;
}
static void *shared(size_t n) {
    void *p = mmap(NULL, n ? n : 1, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (p == MAP_FAILED)
        fail("mmap");
    return p;
}
uint64_t now_ns(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t))
        fail("clock_gettime");
    return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}
void usage_snapshot(Stats *s) {
    if (getrusage(RUSAGE_SELF, &s->usage))
        fail("getrusage");
    FILE *f = fopen("/proc/self/status", "r");
    if (!f)
        fail("proc status");
    char line[256];
    while (fgets(line, sizeof line, f))
        if (sscanf(line, "VmRSS: %ld", &s->rss) == 1)
            break;
    fclose(f);
}
void thread_start(pthread_t *t, void *(*fn)(void *), long id) {
    int e = pthread_create(t, NULL, fn, (void *)id);
    if (e) {
        errno = e;
        fail("pthread_create");
    }
}
static void cleanup(void) {
    for (int i = 0; i < child_count; i++)
        if (children[i] > 0)
            kill(children[i], SIGKILL);
    transport_cleanup();
    while (waitpid(-1, NULL, 0) > 0) {
    }
}
static long number(const char *s, long lo, long hi) {
    char *end;
    errno = 0;
    long n = strtol(s, &end, 10);
    if (errno || !*s || *end || n < lo || n > hi) {
        fprintf(stderr, "invalid number: %s (range %ld..%ld)\n", s, lo, hi);
        exit(2);
    }
    return n;
}
static void *sender(void *arg) {
    long lane = (long)arg;
    int client = lane / WORKERS;
    unsigned char *buf = allocate(chunk_size);
    Stats *s = &stats[lane];
    size_t total = count * (lanes / WORKERS), unique = total - total * duplicate_percent / 100;
    pthread_barrier_wait(start_gate);
    for (size_t j = lane % WORKERS; j < count; j += WORKERS) {
        size_t pos = client * count + j;
        size_t left = file_size - j * chunk_size;
        unsigned len = left < (size_t)chunk_size ? left : (size_t)chunk_size;
        const unsigned char *data = buf;
        if (source)
            data = source + j * chunk_size;
        else {
            uint64_t key = pos % unique;
            memset(buf, (unsigned char)key, len);
            memcpy(buf, &key, sizeof key);
        }
        Request r = {(unsigned)lane, (unsigned)j, len};
        uint64_t begin = now_ns();
        upload(r, data);
        Ack a = receive_ack(lane);
        uint64_t end = now_ns();
        if (a.index != j || a.chunk >= total || a.duplicate > 1)
            fail("invalid acknowledgment");
        if (!s->first)
            s->first = begin;
        s->last = end;
        latency[pos] = end - begin;
    }
    free(buf);
    return NULL;
}
static void client_process(int id) {
    pthread_t threads[WORKERS];
    for (int w = 0; w < WORKERS; w++)
        thread_start(&threads[w], sender, id * WORKERS + w);
    for (int w = 0; w < WORKERS; w++)
        pthread_join(threads[w], NULL);
    usage_snapshot(&stats[id * WORKERS]);
}
static int compare(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}
static double seconds(struct timeval t) { return t.tv_sec + t.tv_usec * 1e-6; }
static void report(void) {
    size_t n = count * (lanes / WORKERS);
    uint64_t first = UINT64_MAX, last = 0, wait = 0, unique = 0, stored = 0;
    double sum = 0, cu = 0, cs = 0;
    long cv = 0, ci = 0, cr = 0;
    for (int i = 0; i < lanes; i++) {
        Stats *s = &stats[i];
        if (s->first && s->first < first)
            first = s->first;
        if (s->last > last)
            last = s->last;
        if (i % WORKERS == 0) {
            cu += seconds(s->usage.ru_utime);
            cs += seconds(s->usage.ru_stime);
            cv += s->usage.ru_nvcsw;
            ci += s->usage.ru_nivcsw;
            cr += s->rss;
        }
    }
    for (int i = 0; i < WORKERS; i++) {
        Stats *s = &stats[lanes + i];
        wait += s->lock_ns;
        unique += s->unique;
        stored += s->stored;
    }
    for (size_t i = 0; i < n; i++)
        sum += latency[i];
    qsort(latency, n, sizeof *latency, compare);
    Stats *srv = &stats[lanes + WORKERS];
    double elapsed = n ? (last - first) * 1e-9 : 0;
    size_t bytes = file_size * (lanes / WORKERS);
    puts(
        "requests,input_bytes,unique_chunks,stored_bytes,elapsed_s,requests_s,mib_s,mean_us,median_"
        "us,p99_us,max_us,lock_wait_us,client_user_s,client_sys_s,client_nvcsw,client_nivcsw,"
        "client_rss_sum_kib,server_user_s,server_sys_s,server_nvcsw,server_nivcsw,server_rss_kib");
    printf("%zu,%zu,%llu,%llu,%.9f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.6f,%.6f,%ld,%ld,%ld,%.6f,%."
           "6f,%ld,%ld,%ld\n",
           n, bytes, (unsigned long long)unique, (unsigned long long)stored, elapsed,
           elapsed ? n / elapsed : 0, elapsed ? bytes / 1048576.0 / elapsed : 0,
           n ? sum / n / 1000 : 0, n ? (latency[(n - 1) / 2] + latency[n / 2]) / 2000. : 0,
           n ? latency[(99 * n + 99) / 100 - 1] / 1000. : 0, n ? latency[n - 1] / 1000. : 0,
           wait / 1000., cu, cs, cv, ci, cr, seconds(srv->usage.ru_utime),
           seconds(srv->usage.ru_stime), srv->usage.ru_nvcsw, srv->usage.ru_nivcsw, srv->rss);
}
int main(int argc, char **argv) {
    if (argc != 7 && argc != 9) {
        fprintf(stderr,
                "usage: %s TEAM CLIENTS CHUNK_BYTES REQUESTS_PER_WORKER DUP_PERCENT global|bucket "
                "[INPUT RESTORE_DIR]\n",
                argv[0]);
        return 2;
    }
    int team = number(argv[1], 0, 99), clients = number(argv[2], 1, 8);
    lanes = clients * WORKERS;
    chunk_size = number(argv[3], 8, 1048576);
    count = WORKERS * number(argv[4], 1, 100000);
    duplicate_percent = number(argv[5], 0, 99);
    if (strcmp(argv[6], "global") && strcmp(argv[6], "bucket"))
        return 2;
    striped = !strcmp(argv[6], "bucket");
    file_size = count * chunk_size;
    if (argc == 9) {
        int fd = open(argv[7], O_RDONLY);
        struct stat st;
        if (fd < 0 || fstat(fd, &st))
            fail("input");
        if (!S_ISREG(st.st_mode))
            return 2;
        file_size = st.st_size;
        count = file_size / chunk_size + (file_size % chunk_size != 0);
        if (count > UINT_MAX / (unsigned)clients)
            return 2;
        if (file_size) {
            source = mmap(NULL, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (source == MAP_FAILED)
                fail("map input");
        }
        close(fd);
        restore_dir = argv[8];
    }
    owner = getpid();
    atexit(cleanup);
    sigset_t set, old;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGCHLD);
    sigprocmask(SIG_BLOCK, &set, &old);
    transport_open(team);
    stats = shared((lanes + WORKERS + 1) * sizeof *stats);
    latency = shared(count * clients * sizeof *latency);
    start_gate = shared(sizeof *start_gate);
    pthread_barrierattr_t attr;
    pthread_barrierattr_init(&attr);
    if (pthread_barrierattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) ||
        pthread_barrier_init(start_gate, &attr, lanes + 1))
        fail("start barrier");
    pthread_barrierattr_destroy(&attr);
    for (int id = -1; id < clients; id++) {
        pid_t p = fork();
        if (p < 0)
            fail("fork");
        if (!p) {
            sigprocmask(SIG_SETMASK, &old, NULL);
            transport_role(id);
            if (id < 0)
                server();
            else
                client_process(id);
            _exit(0);
        }
        children[child_count++] = p;
    }
    transport_role(-2);
    int remaining = child_count;
    while (remaining) {
        int sig;
        if (sigwait(&set, &sig))
            return 1;
        if (sig != SIGCHLD)
            return 128 + sig;
        int status;
        pid_t p;
        while ((p = waitpid(-1, &status, WNOHANG)) > 0) {
            for (int i = 0; i < child_count; i++)
                if (children[i] == p)
                    children[i] = 0;
            remaining--;
            if (!WIFEXITED(status) || WEXITSTATUS(status))
                return 1;
        }
    }
    report();
    return 0;
}
