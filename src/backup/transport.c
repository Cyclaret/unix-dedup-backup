#include "backup.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <unistd.h>
#if BACKEND == 1
static int up[MAX_LANES][2], down[MAX_LANES][2];
static void transfer(int fd, void *buf, size_t len, int sending) {
    unsigned char *p = buf;
    while (len) {
        ssize_t n = sending ? write(fd, p, len) : read(fd, p, len);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            fail("pipe transfer");
        p += n;
        len -= (size_t)n;
    }
}
void transport_open(int team) {
    (void)team;
    for (int i = 0; i < lanes; i++)
        if (pipe(up[i]) || pipe(down[i]))
            fail("pipe");
}
void transport_role(int client) {
    for (int i = 0; i < lanes; i++) {
        if (client != -1) {
            close(up[i][0]);
            close(down[i][1]);
        }
        if (client != i / WORKERS) {
            close(up[i][1]);
            close(down[i][0]);
        }
    }
}
void transport_cleanup(void) {}
void upload(Request r, const unsigned char *p) {
    transfer(up[r.lane][1], &r, sizeof r, 1);
    transfer(up[r.lane][1], (void *)p, r.length, 1);
}
Request receive_chunk(int i, unsigned char *p) {
    Request r;
    transfer(up[i][0], &r, sizeof r, 0);
    if (r.length > (unsigned)chunk_size)
        fail("length");
    transfer(up[i][0], p, r.length, 0);
    return r;
}
void acknowledge(int i, Ack a) { transfer(down[i][1], &a, sizeof a, 1); }
Ack receive_ack(int i) {
    Ack a;
    transfer(down[i][0], &a, sizeof a, 0);
    return a;
}
#elif BACKEND == 2
/* FIFO ordering within each message type preserves fragment order. */
#define FRAGMENT 4096
struct Message {
    long type;
    unsigned char bytes[FRAGMENT];
};
static int qin = -1, qout = -1;
void transport_open(int team) {
    int key = 60000 + 10 * team;
    qin = msgget(key, IPC_CREAT | IPC_EXCL | 0600);
    if (qin < 0)
        fail("request queue");
    qout = msgget(key + 1, IPC_CREAT | IPC_EXCL | 0600);
    if (qout < 0)
        fail("reply queue");
}
void transport_role(int client) { (void)client; }
void transport_cleanup(void) {
    if (qin >= 0)
        msgctl(qin, IPC_RMID, NULL);
    if (qout >= 0)
        msgctl(qout, IPC_RMID, NULL);
}
static void packet(int id, int lane, void *p, size_t n, int sending) {
    struct Message m = {.type = lane + 1};
    if (sending) {
        memcpy(m.bytes, p, n);
        if (msgsnd(id, &m, n, 0) < 0)
            fail("msgsnd");
    } else {
        if (msgrcv(id, &m, n, lane + 1, 0) != (ssize_t)n)
            fail("msgrcv");
        memcpy(p, m.bytes, n);
    }
}
static void payload(int i, unsigned char *p, size_t n, int sending) {
    while (n) {
        size_t step = n < FRAGMENT ? n : FRAGMENT;
        packet(qin, i, p, step, sending);
        p += step;
        n -= step;
    }
}
void upload(Request r, const unsigned char *p) {
    packet(qin, r.lane, &r, sizeof r, 1);
    payload(r.lane, (unsigned char *)p, r.length, 1);
}
Request receive_chunk(int i, unsigned char *p) {
    Request r;
    packet(qin, i, &r, sizeof r, 0);
    if (r.length > (unsigned)chunk_size)
        fail("length");
    payload(i, p, r.length, 0);
    return r;
}
void acknowledge(int i, Ack a) { packet(qout, i, &a, sizeof a, 1); }
Ack receive_ack(int i) {
    Ack a;
    packet(qout, i, &a, sizeof a, 0);
    return a;
}
#else
static int segment = -1, sems = -1;
static unsigned char *area;
static size_t stride;
static void signal_slot(int i, int delta) {
    struct sembuf op = {(unsigned short)i, (short)delta, 0};
    if (semop(sems, &op, 1) < 0)
        fail("semop");
}
void transport_open(int team) {
    int key = 60000 + team * 10;
    stride = sizeof(Request) + sizeof(Ack) + chunk_size;
    segment = shmget(key + 2, stride * lanes, IPC_CREAT | IPC_EXCL | 0600);
    if (segment < 0)
        fail("shmget");
    area = shmat(segment, NULL, 0);
    if (area == (void *)-1)
        fail("shmat");
    sems = semget(key + 3, 2 * lanes, IPC_CREAT | IPC_EXCL | 0600);
    if (sems < 0)
        fail("semget");
    unsigned short zeros[MAX_LANES * 2] = {0};
    union {
        int val;
        unsigned short *array;
    } arg = {.array = zeros};
    if (semctl(sems, 0, SETALL, arg) < 0)
        fail("SETALL");
}
void transport_role(int client) { (void)client; }
void transport_cleanup(void) {
    if (sems >= 0)
        semctl(sems, 0, IPC_RMID);
    if (segment >= 0)
        shmctl(segment, IPC_RMID, NULL);
}
void upload(Request r, const unsigned char *p) {
    unsigned char *slot = area + r.lane * stride;
    memcpy(slot, &r, sizeof r);
    memcpy(slot + sizeof r, p, r.length);
    signal_slot(2 * r.lane, 1);
}
Request receive_chunk(int i, unsigned char *p) {
    Request r;
    unsigned char *slot = area + i * stride;
    signal_slot(2 * i, -1);
    memcpy(&r, slot, sizeof r);
    if (r.length > (unsigned)chunk_size)
        fail("length");
    memcpy(p, slot + sizeof r, r.length);
    return r;
}
void acknowledge(int i, Ack a) {
    memcpy(area + i * stride + sizeof(Request) + chunk_size, &a, sizeof a);
    signal_slot(2 * i + 1, 1);
}
Ack receive_ack(int i) {
    Ack a;
    signal_slot(2 * i + 1, -1);
    memcpy(&a, area + i * stride + sizeof(Request) + chunk_size, sizeof a);
    return a;
}
#endif
