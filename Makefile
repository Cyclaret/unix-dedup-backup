CC = gcc
CFLAGS = -O2 -Wall -Wextra -std=c11 -Iinclude

BUILD_DIR = build

COMMON_SRC = src/common/benchmark.c
COMMON_OBJ = $(BUILD_DIR)/benchmark.o

PIPE_SRC = src/baseline/pipe_test.c
PIPE_BIN = $(BUILD_DIR)/pipe_test

MQ_SRC = src/baseline/mq_test.c
MQ_BIN = $(BUILD_DIR)/mq_test

SHM_SRC = src/baseline/shm_test.c
SHM_BIN = $(BUILD_DIR)/shm_test

.PHONY: all clean

all: $(PIPE_BIN) $(MQ_BIN) $(SHM_BIN)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(COMMON_OBJ): $(COMMON_SRC) include/benchmark.h | $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $(COMMON_SRC) -o $(COMMON_OBJ)

$(PIPE_BIN): $(PIPE_SRC) $(COMMON_OBJ) include/common.h
	$(CC) $(CFLAGS) $(PIPE_SRC) $(COMMON_OBJ) -o $(PIPE_BIN)

$(MQ_BIN): $(MQ_SRC) $(COMMON_OBJ) include/common.h
	$(CC) $(CFLAGS) $(MQ_SRC) $(COMMON_OBJ) -o $(MQ_BIN)

$(SHM_BIN): $(SHM_SRC) $(COMMON_OBJ) include/common.h
	$(CC) $(CFLAGS) $(SHM_SRC) $(COMMON_OBJ) -o $(SHM_BIN)

clean:
	rm -rf $(BUILD_DIR)