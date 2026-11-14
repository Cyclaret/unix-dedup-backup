# 1단계 세 IPC 기법들을 반복 수행하기 위한 자동화 py

#!/usr/bin/env python3

import csv
import random
import re
import statistics
import subprocess
import time

from pathlib import Path
from collections import defaultdict


# ==========================================
# Configuration
# ==========================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BUILD_DIR = PROJECT_ROOT / "build"
RESULT_DIR = PROJECT_ROOT / "results" / "baseline"

RAW_CSV = RESULT_DIR / "raw_results.csv"
SUMMARY_CSV = RESULT_DIR / "summary_results.csv"

IPC_PROGRAMS = {
    "Pipe": BUILD_DIR / "pipe_test",
    "Message Queue": BUILD_DIR / "mq_test",
    "Shared Memory": BUILD_DIR / "shm_test",
}

CHUNK_SIZES = [
    4096,
    16384,
    65536,
    262144,
]

REPEAT_COUNT = 10

# 각 실험 사이 대기 시간
COOLDOWN_SEC = 0.2

# 실행 제한 시간
TIMEOUT_SEC = 60


# ==========================================
# Output Parsing
# ==========================================

def extract_value(pattern, output, value_type=float):
    match = re.search(pattern, output)

    if not match:
        raise ValueError(
            f"Cannot parse pattern: {pattern}\n"
            f"Output:\n{output}"
        )

    return value_type(match.group(1))


def parse_output(output):
    if "Integrity Check: PASS" not in output:
        raise ValueError(
            f"Integrity verification failed:\n{output}"
        )

    elapsed = extract_value(
        r"Elapsed Time:\s*([\d.]+)",
        output,
        float
    )

    throughput = extract_value(
        r"Throughput:\s*([\d.]+)",
        output,
        float
    )

    checksum = extract_value(
        r"Checksum:\s*(\d+)",
        output,
        int
    )

    return {
        "elapsed_sec": elapsed,
        "throughput_mib_s": throughput,
        "checksum": checksum,
    }


# ==========================================
# Benchmark Execution
# ==========================================

def run_single(ipc_name, executable, chunk_size):
    result = subprocess.run(
        [str(executable), str(chunk_size)],
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SEC,
        check=True,
    )

    return parse_output(result.stdout)


def create_test_cases():
    cases = []

    for ipc_name in IPC_PROGRAMS:
        for chunk_size in CHUNK_SIZES:
            for repeat in range(1, REPEAT_COUNT + 1):
                cases.append({
                    "ipc": ipc_name,
                    "chunk_size": chunk_size,
                    "repeat": repeat,
                })

    # 실행 순서 무작위화
    random.shuffle(cases)

    return cases


# ==========================================
# CSV Output
# ==========================================

def save_raw_results(results):
    fieldnames = [
        "ipc",
        "chunk_size",
        "repeat",
        "elapsed_sec",
        "throughput_mib_s",
        "checksum",
    ]

    with open(RAW_CSV, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(results)


def calculate_summary(results):
    grouped = defaultdict(list)

    for row in results:
        key = (
            row["ipc"],
            row["chunk_size"]
        )

        grouped[key].append(row)

    summary = []

    for (ipc, chunk_size), rows in grouped.items():
        elapsed_values = [
            row["elapsed_sec"]
            for row in rows
        ]

        throughput_values = [
            row["throughput_mib_s"]
            for row in rows
        ]

        summary.append({
            "ipc": ipc,
            "chunk_size": chunk_size,
            "count": len(rows),

            "elapsed_mean":
                statistics.mean(elapsed_values),

            "elapsed_std":
                statistics.stdev(elapsed_values)
                if len(elapsed_values) > 1 else 0,

            "throughput_mean":
                statistics.mean(throughput_values),

            "throughput_median":
                statistics.median(throughput_values),

            "throughput_std":
                statistics.stdev(throughput_values)
                if len(throughput_values) > 1 else 0,

            "throughput_min":
                min(throughput_values),

            "throughput_max":
                max(throughput_values),
        })

    return sorted(
        summary,
        key=lambda row: (
            row["chunk_size"],
            row["ipc"]
        )
    )


def save_summary(summary):
    fieldnames = [
        "ipc",
        "chunk_size",
        "count",
        "elapsed_mean",
        "elapsed_std",
        "throughput_mean",
        "throughput_median",
        "throughput_std",
        "throughput_min",
        "throughput_max",
    ]

    with open(SUMMARY_CSV, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(summary)


# ==========================================
# Main
# ==========================================

def main():
    RESULT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # 실행 파일 존재 여부 확인
    for ipc_name, executable in IPC_PROGRAMS.items():
        if not executable.exists():
            raise FileNotFoundError(
                f"{ipc_name} executable not found: "
                f"{executable}"
            )

    cases = create_test_cases()
    results = []

    total = len(cases)

    print("=" * 60)
    print("IPC Baseline Benchmark")
    print("=" * 60)

    print(f"Total Cases: {total}")
    print(f"Repeat Count: {REPEAT_COUNT}")
    print(f"Chunk Sizes: {CHUNK_SIZES}")
    print()

    for index, case in enumerate(cases, start=1):
        ipc_name = case["ipc"]
        chunk_size = case["chunk_size"]
        repeat = case["repeat"]

        executable = IPC_PROGRAMS[ipc_name]

        print(
            f"[{index}/{total}] "
            f"{ipc_name} | "
            f"{chunk_size // 1024} KiB | "
            f"Repeat {repeat}",
            flush=True
        )

        try:
            measurement = run_single(
                ipc_name,
                executable,
                chunk_size
            )

            row = {
                "ipc": ipc_name,
                "chunk_size": chunk_size,
                "repeat": repeat,
                **measurement,
            }

            results.append(row)

            print(
                f"  Elapsed: "
                f"{measurement['elapsed_sec']:.6f} sec | "
                f"Throughput: "
                f"{measurement['throughput_mib_s']:.2f} MiB/s"
            )

        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            ValueError
        ) as e:
            print(f"  ERROR: {e}")
            raise

        # 중간 결과 저장
        save_raw_results(results)

        time.sleep(COOLDOWN_SEC)

    # 통계 계산
    summary = calculate_summary(results)

    save_summary(summary)

    print()
    print("=" * 60)
    print("Benchmark Completed")
    print("=" * 60)

    print(f"Raw Results: {RAW_CSV}")
    print(f"Summary: {SUMMARY_CSV}")

    print()
    print("Average Throughput (MiB/s)")
    print("-" * 60)

    for row in summary:
        print(
            f"{row['ipc']:15s} "
            f"{row['chunk_size'] // 1024:4d} KiB | "
            f"{row['throughput_mean']:8.2f} "
            f"± {row['throughput_std']:.2f}"
        )


if __name__ == "__main__":
    main()