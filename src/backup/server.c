#include "server.h"
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
/* OpenSSL 3 runtime ABI, available on the assignment VM. */
extern unsigned char *SHA256(const unsigned char *, size_t, unsigned char *);
#define BUCKETS 257
typedef struct Chunk {
    struct Chunk *next;
    unsigned char hash[32];
    unsigned length, id, refs;
    unsigned char data[];
} Chunk;
typedef struct {
    Request req;
    unsigned char *data;
} Job;
static Chunk *index_table[BUCKETS], **manifest;
static pthread_mutex_t locks[BUCKETS];
static pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t available = PTHREAD_COND_INITIALIZER;
static Job jobs[MAX_LANES];
static int head, tail, queued, done;
static void *reader(void *arg) {
    long lane = (long)arg;
    unsigned char *buf = allocate(chunk_size);
    for (size_t i = lane % WORKERS; i < count; i += WORKERS) {
        Request req = receive_chunk(lane, buf);
        size_t left = file_size - i * chunk_size;
        if (req.lane != lane || req.index != i ||
            req.length != (left < (size_t)chunk_size ? left : (size_t)chunk_size))
            fail("request metadata");
        pthread_mutex_lock(&queue_lock);
        jobs[tail] = (Job){req, buf};
        tail = (tail + 1) % MAX_LANES;
        queued++;
        pthread_cond_signal(&available);
        pthread_mutex_unlock(&queue_lock);
    }
    /* Buffer lives until server exit: last job may still be queued. */
    return NULL;
}
static void *worker(void *arg) {
    Stats *s = &stats[lanes + (long)arg];
    for (;;) {
        pthread_mutex_lock(&queue_lock);
        while (!queued && !done)
            pthread_cond_wait(&available, &queue_lock);
        if (!queued) {
            pthread_mutex_unlock(&queue_lock);
            break;
        }
        Job job = jobs[head];
        head = (head + 1) % MAX_LANES;
        queued--;
        pthread_mutex_unlock(&queue_lock);
        Request r = job.req;
        unsigned char digest[32];
        if (!SHA256(job.data, r.length, digest))
            fail("SHA256");
        unsigned b = ((unsigned)digest[0] * 256 + digest[1]) % BUCKETS;
        pthread_mutex_t *lock = &locks[striped ? b : 0];
        uint64_t begin = now_ns();
        pthread_mutex_lock(lock);
        s->lock_ns += now_ns() - begin;
        Chunk *c = index_table[b];
        while (c && (c->length != r.length || memcmp(c->hash, digest, 32) ||
                     memcmp(c->data, job.data, r.length)))
            c = c->next;
        unsigned duplicate = c != NULL;
        if (!c) {
            c = allocate(sizeof *c + r.length);
            memcpy(c->hash, digest, 32);
            memcpy(c->data, job.data, r.length);
            c->length = r.length;
            c->id = (r.lane / WORKERS) * count + r.index;
            c->refs = 0;
            c->next = index_table[b];
            index_table[b] = c;
            s->unique++;
            s->stored += r.length;
        }
        c->refs++;
        pthread_mutex_unlock(lock);
        /* Each manifest entry has one writer; chunk contents are immutable. */
        manifest[(r.lane / WORKERS) * count + r.index] = c;
        acknowledge(r.lane, (Ack){r.index, c->id, duplicate});
    }
    return NULL;
}
void server(void) {
    pthread_t pool[WORKERS], inputs[MAX_LANES];
    manifest = allocate((count ? count : 1) * (lanes / WORKERS) * sizeof *manifest);
    for (int i = 0; i < BUCKETS; i++)
        pthread_mutex_init(&locks[i], NULL);
    for (int i = 0; i < WORKERS; i++)
        thread_start(&pool[i], worker, i);
    for (int i = 0; i < lanes; i++)
        thread_start(&inputs[i], reader, i);
    extern pthread_barrier_t *start_gate;
    pthread_barrier_wait(start_gate);
    for (int i = 0; i < lanes; i++)
        pthread_join(inputs[i], NULL);
    pthread_mutex_lock(&queue_lock);
    done = 1;
    pthread_cond_broadcast(&available);
    pthread_mutex_unlock(&queue_lock);
    for (int i = 0; i < WORKERS; i++)
        pthread_join(pool[i], NULL);
    usage_snapshot(&stats[lanes + WORKERS]);
    if (restore_dir)
        for (int i = 0; i < lanes / WORKERS; i++) {
            char *path = allocate(strlen(restore_dir) + 40);
            sprintf(path, "%s/client-%d.bin", restore_dir, i);
            int fd = open(path, O_CREAT | O_EXCL | O_WRONLY, 0600);
            if (fd < 0)
                fail("restore output (use an empty directory)");
            FILE *out = fdopen(fd, "wb");
            if (!out)
                fail("fdopen");
            for (size_t j = 0; j < count; j++) {
                Chunk *c = manifest[i * count + j];
                if (fwrite(c->data, 1, c->length, out) != c->length)
                    fail("restore write");
            }
            if (fclose(out))
                fail("restore close");
            free(path);
        }
}
