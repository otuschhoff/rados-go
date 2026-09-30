# P12 performance policy

P12 uses the four benchmark runs produced while creating a candidate endurance report: Go
and native librados under both secure and CRC transports. Each run must contain
the same unique 36-row matrix formed by four object sizes, three concurrency
levels, and three workloads. The verifier recomputes throughput and IOPS from
the recorded counters before applying these budgets.

| Budget | Limit |
| --- | ---: |
| Minimum Go/native throughput ratio, every row | 0.10 |
| Maximum Go/native p99 latency ratio, every row | 8.0 |
| Maximum Go benchmark RSS, each transport | 2,684,354,560 bytes |
| Maximum Go allocations, each transport | 1,000,000 |
| Maximum Go allocated bytes, each transport | 42,949,672,960 bytes |

The ratio limits are deliberately row-by-row rather than aggregate limits so a
regression in one object-size, concurrency, or workload combination cannot be
hidden by faster unrelated rows. The resource limits provide room for the
current pure-Go implementation and test environment while placing finite caps
on resident memory and allocation churn. They are acceptance guardrails for
this pinned Ceph image, binary set, cluster configuration, and benchmark
methodology.

The endurance resource samples also have explicit growth ceilings relative to
the first sample of each transport: 256 MiB RSS, 128 MiB Go heap, and 256
goroutines. Samples must be strictly ordered by monotonic elapsed time, remain
within the probe duration, and may not exceed either the configured sample
count or the verifier's 2,000-sample hard limit. These bounds detect sustained
growth without treating normal garbage-collection variation as a failure.

These budgets do **not** claim performance parity with native librados. Passing
means only that the measured run stayed within the stated regression and
resource ceilings. Results remain sensitive to hardware, container runtime,
kernel, scheduling, storage, and the small benchmark operation count.