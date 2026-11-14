CC = gcc
CFLAGS = -O2 -Wall -Wextra -std=c11 -pthread -Iinclude
BUILD_DIR = build
PIPE_BENCH_SRC = src/baseline/pipe_bench.c
METRICS_SRC = src/common/bench_metrics.c
PIPE_BENCH_BIN = $(BUILD_DIR)/pipe_bench
.PHONY: all clean
all: $(PIPE_BENCH_BIN)
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)
$(PIPE_BENCH_BIN): $(PIPE_BENCH_SRC) $(METRICS_SRC) include/ipc_bench.h include/bench_metrics.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(PIPE_BENCH_SRC) $(METRICS_SRC) -o $(PIPE_BENCH_BIN)
clean:
	rm -rf $(BUILD_DIR)
