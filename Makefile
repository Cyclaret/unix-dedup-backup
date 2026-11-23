CC = gcc
CFLAGS = -O2 -Wall -Wextra -std=c11 -pthread -Iinclude

BUILD_DIR = build
METRICS_SRC = src/common/bench_metrics.c

PIPE_BENCH_SRC = src/baseline/pipe_bench.c
MQ_BENCH_SRC = src/baseline/mq_bench.c
SHM_BENCH_SRC = src/baseline/shm_bench.c

PIPE_BENCH_BIN = $(BUILD_DIR)/pipe_bench
MQ_BENCH_BIN = $(BUILD_DIR)/mq_bench
SHM_BENCH_BIN = $(BUILD_DIR)/shm_bench

.PHONY: all clean

# Build all IPC benchmarks
all: $(PIPE_BENCH_BIN) $(MQ_BENCH_BIN) $(SHM_BENCH_BIN)

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Pipe benchmark
$(PIPE_BENCH_BIN): $(PIPE_BENCH_SRC) $(METRICS_SRC) include/ipc_bench.h include/bench_metrics.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(PIPE_BENCH_SRC) $(METRICS_SRC) -o $(PIPE_BENCH_BIN)

# Message Queue benchmark
$(MQ_BENCH_BIN): $(MQ_BENCH_SRC) $(METRICS_SRC) include/ipc_bench.h include/bench_metrics.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(MQ_BENCH_SRC) $(METRICS_SRC) -o $(MQ_BENCH_BIN)

# Shared Memory benchmark
$(SHM_BENCH_BIN): $(SHM_BENCH_SRC) $(METRICS_SRC) include/ipc_bench.h include/bench_metrics.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(SHM_BENCH_SRC) $(METRICS_SRC) -o $(SHM_BENCH_BIN)

# Clean build files
clean:
	rm -rf $(BUILD_DIR)

# Phase 2: all three binaries share the application and select one transport.
BACKUP_SRC := src/backup/main.c src/backup/server.c src/backup/transport.c
BACKUP_HEADERS := src/backup/backup.h src/backup/server.h
.PHONY: backup phase1
phase1: $(PIPE_BENCH_BIN) $(MQ_BENCH_BIN) $(SHM_BENCH_BIN)
all: backup
backup: build/pipe_backup build/mq_backup build/shm_backup
build/pipe_backup: BACKEND_ID=1
build/mq_backup: BACKEND_ID=2
build/shm_backup: BACKEND_ID=3
build/pipe_writev_backup: BACKEND_ID=1
build/pipe_writev_backup: EXTRA_FLAGS=-DPIPE_WRITEV
build/mq_8k_backup: BACKEND_ID=2
build/mq_8k_backup: EXTRA_FLAGS=-DFRAGMENT=8192
build/pipe_backup build/mq_backup build/shm_backup build/pipe_writev_backup build/mq_8k_backup: $(BACKUP_SRC) $(BACKUP_HEADERS) | $(BUILD_DIR)
	$(CC) -std=gnu11 -O2 -Wall -Wextra -pthread $(EXTRA_FLAGS) -DBACKEND=$(BACKEND_ID) $(BACKUP_SRC) -l:libcrypto.so.3 -o $@
