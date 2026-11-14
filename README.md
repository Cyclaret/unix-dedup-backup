UNIX Deduplicated Backup Server

UNIX 시스템 프로그래밍 설계 프로젝트

1. 프로젝트 개요

본 프로젝트는 UNIX 환경에서 IPC(Inter-Process Communication) 방식에 따른 성능 차이를 분석하고, 이를 중복 제거 백업 서버에 적용하여 워크로드별 IPC 효율성을 비교하는 것을 목표로 한다.

비교 대상은 다음 세 가지다.

* Pipe
* System V Message Queue
* System V Shared Memory

프로젝트는 두 단계로 진행한다.

단계	내용	상태
STEP 1	IPC 기본 성능 비교 및 분석	완료
STEP 2	중복 제거 백업 서버 구현 및 성능 평가	예정

2. 프로젝트 구조
```text
unix-dedup-backup/
├── include/
│   ├── bench_metrics.h
│   └── ipc_bench.h
├── src/
│   ├── baseline/
│   │   ├── pipe_bench.c
│   │   ├── mq_bench.c
│   │   └── shm_bench.c
│   └── common/
│       └── bench_metrics.c
├── scripts/
│   └── run_benchmarks.py
├── results/
│   ├── baseline_error/
│   ├── baseline_final/
│   └── environment.txt
├── build/
├── Makefile
└── README.md
```
build/는 컴파일 시 생성되며 Git 추적 대상에서 제외한다.

3. 개발 환경

* OS: Ubuntu Linux (ARM64 VM)
* Language: C11, Python 3
* Compiler: GCC
* Build: GNU Make
* Concurrency: fork(), POSIX Threads
* IPC: Pipe, System V Message Queue, System V Shared Memory
* Synchronization: Mutex, Semaphore

세부 실행 환경은 results/environment.txt를 참고한다.

4. STEP 1 — IPC Baseline Benchmark

구현 내용

서버 프로세스 1개와 여러 클라이언트 프로세스를 생성하고, 각 프로세스에서 Worker 스레드 3개를 실행한다.

각 Worker는 IPC를 통해 요청·응답 데이터를 교환한다.

측정 지표는 다음과 같다.

* RTT: Mean, Median, P99, Max
* Throughput: Messages/sec, MiB/s
* CPU: User/System Time
* Context Switch: Voluntary/Involuntary
* Memory: RSS
* Data Integrity

실험 조건

항목	설정
IPC	Pipe, MQ, SHM
메시지 크기	64B, 1KiB, 16KiB, 256KiB, 1MiB
클라이언트 수	1, 2, 4, 8
Worker 수	프로세스당 3개
반복 횟수	3회
측정 요청	Worker당 100회
전체 실행	180회

실험 결과

총 180회 실험이 모두 정상 완료되었다.

주요 관찰 결과는 다음과 같다.

1. Pipe는 소형 메시지(64B~16KiB)에서 높은 처리량을 기록했다.
2. SHM은 대형 메시지(256KiB~1MiB)에서 높은 처리량을 기록했다.
3. MQ는 대형 메시지 전송 시 분할 처리 오버헤드로 인해 상대적으로 낮은 처리량을 기록했다.
4. 클라이언트 수 증가에 따른 확장성은 IPC와 메시지 크기에 따라 달랐다.

실험 결과는 results/baseline_final/에 저장되어 있다.

본 결과는 현재 구현과 실험 환경에서 측정한 예비 성능 비교이며, 모든 UNIX 환경에서 동일한 성능 순위를 보장하지 않는다.

5. 빌드 및 실행

전체 빌드

make

빌드 파일 삭제

make clean

개별 IPC 테스트

사용법:

./build/<ipc>_bench <message_size> <clients> <requests_per_worker>

예시:

./build/pipe_bench 4096 2 100
./build/mq_bench 4096 2 100
./build/shm_bench 4096 2 100

자동 벤치마크

python3 scripts/run_benchmarks.py \
    --repeats 3 \
    --requests 100 \
    --results-dir results/baseline_final

기존 결과를 보존하려면 새로운 결과 디렉터리를 지정한다.

실험 결과로 raw_results.csv, summary_results.csv가 생성된다. 실패한 실험이 있으면 errors.log에서 확인할 수 있다.

6. STEP 2 — Deduplicated Backup Server

STEP 1의 결과를 바탕으로 중복 제거 백업 서버를 구현할 예정이다.

기본 처리 과정:

1. 클라이언트가 파일을 청크 단위로 분할한다.
2. IPC를 통해 서버에 청크 데이터를 전달한다.
3. 서버가 청크 해시를 계산한다.
4. 공유 해시 인덱스에서 중복 여부를 검사한다.
5. 중복 청크는 참조 정보를 갱신하고, 신규 청크는 저장한다.
6. 처리 결과를 클라이언트에 반환한다.

주요 실험 변수:

* IPC 방식
* 청크 크기
* 클라이언트 수
* 데이터 중복률

STEP 1에서 관찰한 IPC 성능 특성이 실제 백업 워크로드에서도 유지되는지 검증한다.

7. 참고 사항

* MQ는 대형 메시지 전송을 위해 메시지를 분할한다.
* SHM은 공유 메모리와 동기화 기법을 사용한다.
* 벤치마크의 처리량은 로컬 IPC에서 계산한 논리적 데이터 처리량이다.
* STEP 2의 설계와 구현 내용은 개발 진행에 따라 갱신한다.
