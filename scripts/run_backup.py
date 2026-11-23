"""Run from the repository root after reviewing the implementation."""
import argparse
import csv
import itertools
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--team', required=True, type=int)
parser.add_argument('--output', required=True, type=Path)
parser.add_argument('--repeats', type=int, default=30)
parser.add_argument('--requests', type=int, default=10)
parser.add_argument('--sizes', nargs='+', type=int, default=[64,4096,16384,262144,1048576])
parser.add_argument('--clients', nargs='+', type=int, default=[1,2,4,8])
parser.add_argument('--duplicates', nargs='+', type=int, default=[0,50,90])
parser.add_argument('--locks', nargs='+', choices=['global','bucket'], default=['global'])
args = parser.parse_args()
if args.repeats < 1:
    parser.error('repeats must be positive')
args.output.parent.mkdir(parents=True, exist_ok=True)
with args.output.open('x', newline='') as output:
    writer = None
    for size, clients, dup, lock in itertools.product(args.sizes, args.clients, args.duplicates, args.locks):
        modes = ['pipe', 'mq', 'shm']
        for trial in range(args.repeats + 1):
            for k in range(3):
                mode = modes[(trial+k)%3]
                cmd = [f'./build/{mode}_backup',args.team,clients,size,args.requests,dup,lock]
                result = subprocess.run(list(map(str,cmd)),check=True,stdout=subprocess.PIPE,text=True)
                metrics, = csv.DictReader(result.stdout.splitlines())
                if not trial:
                    continue
                row = dict(ipc=mode,chunk_bytes=size,clients=clients,requests_per_worker=args.requests,
                           duplicate_percent=dup,lock=lock,trial=trial,**metrics)
                if writer is None:
                    writer = csv.DictWriter(output,fieldnames=list(row))
                    writer.writeheader()
                writer.writerow(row)
                output.flush()
