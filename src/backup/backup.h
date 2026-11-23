#pragma once
#include <stddef.h>
#include <stdint.h>
#define MAX_LANES 24
#define WORKERS 3
/* A lane identifies client and worker. Each client backs up one file. */
typedef struct {
    uint32_t lane, index, length;
} Request;
typedef struct {
    uint32_t index, chunk, duplicate;
} Ack;
extern int lanes, chunk_size;
void fail(const char *what);
void transport_open(int team);
void transport_role(int client); /* -1 is server */
void transport_cleanup(void);
void upload(Request req, const unsigned char *data);
Request receive_chunk(int lane, unsigned char *data);
void acknowledge(int lane, Ack ack);
Ack receive_ack(int lane);
