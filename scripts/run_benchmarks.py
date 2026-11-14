# 1단계 IPC 세 기법 비교용 자동화 py

#!/usr/bin/env python3
"""Run Pipe/MQ/SHM benchmarks, persist per-run CSV, and summarize successful runs."""
import argparse
import csv
import datetime as dt
import json
import math
import random
import re
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
IPCS = {'pipe': 'pipe_bench', 'mq': 'mq_bench', 'shm': 'shm_bench'}
SIZES = (64, 1024, 16384, 262144, 1048576)
CLIENTS = (1, 2, 4, 8)
FIELDS = [
    'rtt_mean_us', 'rtt_median_us', 'rtt_p99_us', 'rtt_max_us',
    'messages_per_sec', 'throughput_mib_sec',
    'client_cpu_user_sec', 'client_cpu_system_sec',
    'client_voluntary_cs', 'client_involuntary_cs',
    'client_peak_rss_kb', 'client_current_rss_kb',
    'server_cpu_user_sec', 'server_cpu_system_sec',
    'server_voluntary_cs', 'server_involuntary_cs',
    'server_peak_rss_kb', 'server_current_rss_kb',
]
RAW_FIELDS = ['timestamp', 'ipc', 'message_size', 'clients', 'repeat',
              'requests_per_worker', 'workers_per_process', 'completed_requests',
              'elapsed_sec', 'status', 'return_code', 'error'] + FIELDS
SUMMARY_FIELDS = ['ipc', 'message_size', 'clients', 'requests_per_worker',
                  'successful_runs', 'failed_runs'] + [f'{f}_{s}' for f in FIELDS for s in ('mean', 'stddev')]


def value(text, label, unit=''):
    pattern = rf'(?m)^\s*{re.escape(label)}:\s*([0-9]+(?:\.[0-9]+)?)\s*{re.escape(unit)}\s*$'
    match = re.search(pattern, text)
    if not match:
        raise ValueError(f'missing field: {label}')
    return float(match.group(1))


def parse_output(output, ipc, size, clients, requests):
    expected = clients * 3 * requests
    if 'Shared Statistics: PASS' not in output or 'Integrity Check: PASS' not in output:
        raise ValueError('integrity/shared-statistics check missing or failed')
    actual_ipc = re.search(r'(?m)^IPC:\s*(.+)$', output)
    expected_names = {'pipe': 'Pipe', 'mq': 'Message Queue (System V)',
                      'shm': 'Shared Memory (System V)'}
    if not actual_ipc or actual_ipc.group(1).strip() != expected_names[ipc]:
        raise ValueError('IPC label mismatch')
    if int(value(output, 'Message Size')) != size or int(value(output, 'Clients')) != clients:
        raise ValueError('size/client mismatch')
    workers = int(value(output, 'Workers per Process'))
    if workers != 3 or int(value(output, 'Measure Requests per Worker')) != requests:
        raise ValueError('worker/request mismatch')
    completed = int(value(output, 'Completed Requests'))
    if completed != expected:
        raise ValueError(f'completed requests {completed} != {expected}')
    elapsed = value(output, 'Elapsed Time', 'sec')
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise ValueError('invalid elapsed time')
    metrics = {
        'rtt_mean_us': value(output, 'RTT Mean', 'us'),
        'rtt_median_us': value(output, 'RTT Median', 'us'),
        'rtt_p99_us': value(output, 'RTT P99', 'us'),
        'rtt_max_us': value(output, 'RTT Max', 'us'),
        'messages_per_sec': value(output, 'Messages/sec'),
        'throughput_mib_sec': value(output, 'Throughput', 'MiB/s'),
    }
    sections = output.split('[Client Resources]')
    if len(sections) != 2 or '[Server Resources]' not in sections[1]:
        raise ValueError('resource sections missing')
    client_text, server_text = sections[1].split('[Server Resources]', 1)
    for prefix, part in [('client', client_text), ('server', server_text)]:
        metrics.update({
            f'{prefix}_cpu_user_sec': value(part, 'CPU User', 'sec'),
            f'{prefix}_cpu_system_sec': value(part, 'CPU System', 'sec'),
            f'{prefix}_voluntary_cs': value(part, 'Voluntary CS'),
            f'{prefix}_involuntary_cs': value(part, 'Involuntary CS'),
            f'{prefix}_peak_rss_kb': value(part, 'Peak RSS Sum' if prefix == 'client' else 'Peak RSS', 'kB'),
            f'{prefix}_current_rss_kb': value(part, 'Current RSS Sum' if prefix == 'client' else 'Current RSS', 'kB'),
        })
    return {'workers_per_process': workers, 'completed_requests': completed,
            'elapsed_sec': elapsed, **metrics}


def append_row(path, fields, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists() or path.stat().st_size == 0
    with path.open('a', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if new:
            writer.writeheader()
        writer.writerow({key: row.get(key, '') for key in fields})
        handle.flush()


def load_rows(path):
    if not path.exists():
        return []
    with path.open(newline='', encoding='utf-8') as handle:
        return list(csv.DictReader(handle))


def summarize(rows, output):
    grouped = defaultdict(list)
    for row in rows:
        key = (row['ipc'], int(row['message_size']), int(row['clients']),
               int(row['requests_per_worker']))
        grouped[key].append(row)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        for key in sorted(grouped):
            group = grouped[key]
            success = [r for r in group if r['status'] == 'ok']
            item = dict(zip(('ipc', 'message_size', 'clients', 'requests_per_worker'), key))
            item['successful_runs'] = len(success)
            item['failed_runs'] = len(group) - len(success)
            for field in FIELDS:
                values = [float(r[field]) for r in success]
                item[f'{field}_mean'] = statistics.mean(values) if values else ''
                item[f'{field}_stddev'] = statistics.stdev(values) if len(values) > 1 else (0 if values else '')
            writer.writerow(item)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repeats', type=int, default=30)
    parser.add_argument('--requests', type=int, default=1000, help='measured requests per worker')
    parser.add_argument('--timeout', type=float, default=660, help='timeout per run in seconds')
    parser.add_argument('--seed', type=int, default=2026)
    parser.add_argument('--results-dir', type=Path, default=ROOT / 'results')
    parser.add_argument('--resume', action='store_true', help='skip successful runs already recorded')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if args.repeats <= 0 or args.requests <= 0 or args.timeout <= 0:
        parser.error('repeats, requests and timeout must be positive')
    results = args.results_dir.resolve()
    raw = results / 'raw_results.csv'
    summary = results / 'summary_results.csv'
    error_log = results / 'errors.log'
    if raw.exists() and not args.resume and not args.dry_run:
        parser.error(f'{raw} already exists; use --resume or a new --results-dir')
    if not args.dry_run:
        missing = [str(ROOT / 'build' / exe) for exe in IPCS.values()
                   if not (ROOT / 'build' / exe).is_file()]
        if missing:
            parser.error('missing executables; run make first: ' + ', '.join(missing))
    existing = load_rows(raw) if args.resume else []
    successful = {(r['ipc'], int(r['message_size']), int(r['clients']), int(r['repeat']),
                   int(r['requests_per_worker'])) for r in existing if r['status'] == 'ok'}
    tasks = [(ipc, size, clients, rep) for ipc in IPCS for size in SIZES
             for clients in CLIENTS for rep in range(1, args.repeats + 1)
             if (ipc, size, clients, rep, args.requests) not in successful]
    random.Random(args.seed).shuffle(tasks)
    print(f'Tasks to execute: {len(tasks)} / {len(IPCS)*len(SIZES)*len(CLIENTS)*args.repeats}', flush=True)
    if args.dry_run:
        for task in tasks[:10]:
            print(task)
        return 0
    for index, (ipc, size, clients, rep) in enumerate(tasks, 1):
        cmd = [str(ROOT / 'build' / IPCS[ipc]), str(size), str(clients), str(args.requests)]
        row = {'timestamp': dt.datetime.now(dt.timezone.utc).isoformat(),
               'ipc': ipc, 'message_size': size, 'clients': clients,
               'repeat': rep, 'requests_per_worker': args.requests,
               'status': 'error', 'return_code': '', 'error': ''}
        try:
            result = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True,
                                    timeout=args.timeout, errors='replace')
            row['return_code'] = result.returncode
            if result.returncode != 0:
                raise RuntimeError(f'exit {result.returncode}: {result.stderr[-500:]}')
            row.update(parse_output(result.stdout, ipc, size, clients, args.requests))
            row['status'] = 'ok'
        except (subprocess.TimeoutExpired, ValueError, RuntimeError, OSError) as exc:
            row['error'] = str(exc).replace('\n', ' ')[:1000]
            results.mkdir(parents=True, exist_ok=True)
            with error_log.open('a', encoding='utf-8') as handle:
                handle.write(json.dumps({'task': [ipc, size, clients, rep], 'error': row['error']}, ensure_ascii=False) + '\n')
        append_row(raw, RAW_FIELDS, row)
        print(f'[{index}/{len(tasks)}] {ipc:4} {size:7}B clients={clients} '
              f'repeat={rep:2} {row["status"]}' + (f' ({row["error"]})' if row['error'] else ''), flush=True)
    summarize(load_rows(raw), summary)
    print(f'Raw: {raw}\nSummary: {summary}\nErrors: {error_log}', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
