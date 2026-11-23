UNIX Phase 2 — in-memory deduplicating backup server
Run in Ubuntu (gcc, GNU make, Python 3, glibc pthreads, OpenSSL 3 runtime libcrypto.so.3).
Record environment with lscpu, free -h, uname -a, cat /etc/os-release, gcc --version, systemd-detect-virt.
Build all: make. Phase 1: make phase1. Phase 2: make backup.
Individual: make build/pipe_backup build/mq_backup build/shm_backup (select any one target).
Phase 1 commands and CSV format: README.md and scripts/run_benchmarks.py.

Review before running: src/backup/main.c (clients, lifecycle, metrics), server.c (queue, store, restore), transport.c (IPC), scripts/run_backup.py.
CLI: ./build/{pipe,mq,shm}_backup TEAM CLIENTS CHUNK_BYTES REQUESTS_PER_WORKER DUP_PERCENT global|bucket [INPUT RESTORE_DIR]
TEAM is the actual team number (00..99). Keys are 60000+10*TEAM plus 0/1 for MQ and 2/3 for SHM and semaphores. IPC_EXCL prevents replacing existing objects. Run only one instance of the same backend/team at a time.
Different clients back up the same input in file mode. Manifest positions keep the final partial chunk and original order. Each restored client-N.bin must equal INPUT. Output directory must exist, and filenames must be new. Empty and binary files are supported. REQUESTS and DUP are ignored for file contents. The memory store lasts for one session; restored files persist.

Functional handoff (bash):
read -rp 'Team number: ' TEAM
make
D=$(mktemp -d)
head -c 4096 /dev/urandom > "$D/chunk"
(cat "$D/chunk" "$D/chunk"; printf END) > "$D/input"
ipcs > "$D/ipcs.before"
for ipc in pipe mq shm; do
  mkdir "$D/$ipc" || break
  ./build/${ipc}_backup "$TEAM" 2 4096 1 0 global "$D/input" "$D/$ipc" || break
  cmp "$D/input" "$D/$ipc/client-0.bin" && cmp "$D/input" "$D/$ipc/client-1.bin" || break
done
ipcs > "$D/ipcs.after"
diff -u "$D/ipcs.before" "$D/ipcs.after"
Expected per IPC: 6 requests, 16390 input bytes, 2 unique chunks, 4099 stored bytes, both cmp commands silent. Confirm all 3 binaries ran. Repeat with an empty file and new restore directories; expect zero requests. To exercise many lanes use 8 clients.

Measurement after review:
python3 scripts/run_backup.py --team "$TEAM" --output results/phase2/smoke.csv --sizes 4096 --clients 2 --duplicates 50 --repeats 1
python3 scripts/run_backup.py --team "$TEAM" --output results/phase2/global.csv
python3 scripts/run_backup.py --team "$TEAM" --output results/phase2/locks.csv --locks global bucket --clients 4 --sizes 262144 --duplicates 50
Output CSV uses exclusive creation and retains completed trials. Warm-up trial excluded; default 30 independent fresh processes per condition. Mode order rotates. Ten requests per client worker by default. Each lane sends full data, waits for ACK, then continues. Synthetic distinct count = N-floor(N*dup/100); ids cycle through this many keys across all clients. No disk I/O in synthetic runs. Separate real-file runs validate restore.
RTT begins immediately before upload and ends after ACK; throughput uses logical input bytes from earliest send to last ACK across all clients. Latencies exclude data generation; elapsed time includes generation between requests. Report mean/median/nearest-rank P99/max in microseconds. Lock wait is summed over all requests. CPU and context switches cover process lifetime to snapshot (server before restore); RSS is sampled at snapshot, client RSS summed across processes. These are different intervals from throughput. Compare unique storage and RSS separately. Fixed work, repeated single-size trials, same VM, do not run other heavy tasks. Use strace -f -c on a separate diagnostic run if installed. Compare global vs bucket with all other factors fixed, then report the observed improvement or regression. Keep raw CSVs, environment, ipcs and both phases' commands for submission.
