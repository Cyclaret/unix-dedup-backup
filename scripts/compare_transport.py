"""Controlled transport comparisons. Run from the project root with a new output path."""
import csv
import itertools
from pathlib import Path
import statistics
import subprocess
import sys
from tempfile import TemporaryDirectory

team, destination = sys.argv[1:]
experiments = [(['pipe', 'pipe_writev', 'shm'], 64, 100), (['mq', 'mq_8k'], 1048576, 10)]

def invoke(mode, size, requests, extra=()):
    command = [f'build/{mode}_backup', team, '4', str(size), str(requests), '50', 'global', *extra]
    data = subprocess.check_output(command, text=True, timeout=60)
    row, = csv.DictReader(data.splitlines())
    return row

with TemporaryDirectory() as directory:
    root = Path(directory)
    chunk = bytes(range(256)) * 4096
    original = chunk * 2 + b'END'
    source = root / 'input'
    source.write_bytes(original)
    for mode in ('pipe','pipe_writev','shm','mq','mq_8k'):
        folder = root / mode
        folder.mkdir()
        row = invoke(mode,len(chunk),1,[source,folder])
        assert int(row['unique_chunks']) == 2 and int(row['stored_bytes']) == len(chunk)+3
        for client in range(4):
            assert (folder / f'client-{client}.bin').read_bytes() == original
print('RESTORE PASS (5 modes, 4 clients)',flush=True)

with open(destination,'x',newline='') as output:
    writer = None
    for modes,size,requests in experiments:
        orders = list(itertools.permutations(modes))
        collected = {m: [] for m in modes}
        for trial in range(31):
            for mode in orders[(trial-1) % len(orders)]:
                row = invoke(mode,size,requests)
                total = 12 * requests
                assert int(row['requests']) == total and int(row['input_bytes']) == total*size
                assert int(row['unique_chunks']) == total//2 and int(row['stored_bytes']) == total*size//2
                if trial:
                    record = dict(mode=mode,trial=trial,size=size,requests_per_worker=requests,**row)
                    if writer is None:
                        writer=csv.DictWriter(output,fieldnames=list(record)); writer.writeheader()
                    writer.writerow(record); output.flush(); collected[mode].append(row)
        for mode,rows in collected.items():
            values=[float(r['mib_s']) for r in rows]
            q=statistics.quantiles(values,n=4,method='inclusive')
            print(mode, 'Q1/median/Q3',*[round(x,3) for x in q], 'P99_us',statistics.median(float(r['p99_us']) for r in rows),flush=True)
