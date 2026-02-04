//! Benchmark comparing EagleDB vs Redis for common commands
//!
//! This benchmark measures the performance of EagleDB against Redis for:
//! - GET/SET operations (single key)
//! - MGET/MSET operations (bulk operations)
//! - INCR/DECR operations (atomic counters)
//! - Hash operations (HSET/HGET)
//! - Mixed workloads (read-heavy, write-heavy, balanced)
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//! - EagleDB server running on localhost:6380
//!
//! Run with: cargo bench --bench redis_comparison

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::Rng;
use redis::Commands;
use std::hint::black_box;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::time::Duration;

const REDIS_ADDR: &str = "127.0.0.1:6379";
const EAGLE_ADDR: &str = "127.0.0.1:6380";

/// Simple RESP protocol client for EagleDB
struct EagleClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl EagleClient {
    fn connect(addr: &str) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let reader = BufReader::new(stream.try_clone()?);
        let writer = BufWriter::new(stream);
        Ok(Self { reader, writer })
    }

    fn send_command(&mut self, args: &[&[u8]]) -> std::io::Result<String> {
        // Write RESP array
        write!(self.writer, "*{}\r\n", args.len())?;
        for arg in args {
            write!(self.writer, "${}\r\n", arg.len())?;
            self.writer.write_all(arg)?;
            self.writer.write_all(b"\r\n")?;
        }
        self.writer.flush()?;

        // Read response
        let mut line = String::new();
        self.reader.read_line(&mut line)?;
        Ok(line)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.send_command(&[b"SET", key, value])?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let response = self.send_command(&[b"GET", key])?;
        if let Some(stripped) = response.strip_prefix('$') {
            let len: i64 = stripped.trim().parse().unwrap_or(-1);
            if len < 0 {
                return Ok(None);
            }
            let mut buf = vec![0u8; len as usize];
            std::io::Read::read_exact(&mut self.reader, &mut buf)?;
            let mut crlf = [0u8; 2];
            std::io::Read::read_exact(&mut self.reader, &mut crlf)?;
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    fn mset(&mut self, pairs: &[(&[u8], &[u8])]) -> std::io::Result<()> {
        let mut args: Vec<&[u8]> = vec![b"MSET"];
        for (k, v) in pairs {
            args.push(k);
            args.push(v);
        }
        self.send_command(&args)?;
        Ok(())
    }

    fn mget(&mut self, keys: &[&[u8]]) -> std::io::Result<Vec<Option<Vec<u8>>>> {
        let mut args: Vec<&[u8]> = vec![b"MGET"];
        args.extend(keys);
        let response = self.send_command(&args)?;

        // Parse RESP array response
        let mut results = Vec::with_capacity(keys.len());
        if let Some(stripped) = response.strip_prefix('*') {
            let count: usize = stripped.trim().parse().unwrap_or(0);
            for _ in 0..count {
                let mut line = String::new();
                self.reader.read_line(&mut line)?;
                if let Some(len_str) = line.strip_prefix('$') {
                    let len: i64 = len_str.trim().parse().unwrap_or(-1);
                    if len < 0 {
                        results.push(None);
                    } else {
                        let mut buf = vec![0u8; len as usize];
                        std::io::Read::read_exact(&mut self.reader, &mut buf)?;
                        let mut crlf = [0u8; 2];
                        std::io::Read::read_exact(&mut self.reader, &mut crlf)?;
                        results.push(Some(buf));
                    }
                } else {
                    results.push(None);
                }
            }
        }
        Ok(results)
    }

    fn incr(&mut self, key: &[u8]) -> std::io::Result<i64> {
        let response = self.send_command(&[b"INCR", key])?;
        if let Some(stripped) = response.strip_prefix(':') {
            Ok(stripped.trim().parse().unwrap_or(0))
        } else {
            Ok(0)
        }
    }

    fn hset(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.send_command(&[b"HSET", key, field, value])?;
        Ok(())
    }

    fn hget(&mut self, key: &[u8], field: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let response = self.send_command(&[b"HGET", key, field])?;
        if let Some(stripped) = response.strip_prefix('$') {
            let len: i64 = stripped.trim().parse().unwrap_or(-1);
            if len < 0 {
                return Ok(None);
            }
            let mut buf = vec![0u8; len as usize];
            std::io::Read::read_exact(&mut self.reader, &mut buf)?;
            let mut crlf = [0u8; 2];
            std::io::Read::read_exact(&mut self.reader, &mut crlf)?;
            Ok(Some(buf))
        } else {
            Ok(None)
        }
    }

    #[allow(dead_code)]
    fn del(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.send_command(&[b"DEL", key])?;
        Ok(())
    }

    #[allow(dead_code)]
    fn flushdb(&mut self) -> std::io::Result<()> {
        self.send_command(&[b"FLUSHDB"])?;
        Ok(())
    }
}

/// Check if Redis is available
fn redis_available() -> bool {
    redis::Client::open(format!("redis://{}/", REDIS_ADDR))
        .and_then(|c| c.get_connection())
        .is_ok()
}

/// Check if EagleDB is available
fn eagle_available() -> bool {
    EagleClient::connect(EAGLE_ADDR).is_ok()
}

/// Generate random value of specified size
fn random_value(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random::<u8>()).collect()
}

/// Benchmark GET/SET operations
fn bench_get_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_set");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let value_sizes = [64, 256, 1024, 4096];

    for size in value_sizes {
        let value = random_value(size);

        // Benchmark Redis
        if redis_available() {
            let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
            let mut con = client.get_connection().unwrap();

            group.bench_with_input(BenchmarkId::new("redis_set", size), &size, |b, _| {
                let key = format!("bench:set:{}", size);
                b.iter(|| {
                    let _: () = con.set(black_box(&key), black_box(&value)).unwrap();
                });
            });

            // Pre-populate for GET benchmark
            let key = format!("bench:get:{}", size);
            let _: () = con.set(&key, &value).unwrap();

            group.bench_with_input(BenchmarkId::new("redis_get", size), &size, |b, _| {
                b.iter(|| {
                    let _: Vec<u8> = con.get(black_box(&key)).unwrap();
                });
            });
        }

        // Benchmark EagleDB
        if eagle_available() {
            let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

            group.bench_with_input(BenchmarkId::new("eagle_set", size), &size, |b, _| {
                let key = format!("bench:set:{}", size);
                b.iter(|| {
                    client
                        .set(black_box(key.as_bytes()), black_box(&value))
                        .unwrap();
                });
            });

            // Pre-populate for GET benchmark
            let key = format!("bench:get:{}", size);
            client.set(key.as_bytes(), &value).unwrap();

            group.bench_with_input(BenchmarkId::new("eagle_get", size), &size, |b, _| {
                b.iter(|| {
                    client.get(black_box(key.as_bytes())).unwrap();
                });
            });
        }
    }

    group.finish();
}

/// Benchmark MGET/MSET operations (bulk)
fn bench_mget_mset(c: &mut Criterion) {
    let mut group = c.benchmark_group("mget_mset");
    group.measurement_time(Duration::from_secs(10));

    let batch_sizes = [10, 50, 100];
    let value_size = 256;

    for batch_size in batch_sizes {
        group.throughput(Throughput::Elements(batch_size as u64));

        let keys: Vec<String> = (0..batch_size)
            .map(|i| format!("bench:bulk:{}", i))
            .collect();
        let values: Vec<Vec<u8>> = (0..batch_size).map(|_| random_value(value_size)).collect();

        // Benchmark Redis MSET
        if redis_available() {
            let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
            let mut con = client.get_connection().unwrap();

            let pairs: Vec<(&str, &[u8])> = keys
                .iter()
                .zip(values.iter())
                .map(|(k, v)| (k.as_str(), v.as_slice()))
                .collect();

            group.bench_with_input(
                BenchmarkId::new("redis_mset", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        let _: () = redis::cmd("MSET").arg(&pairs).query(&mut con).unwrap();
                    });
                },
            );

            // Pre-populate for MGET
            let _: () = redis::cmd("MSET").arg(&pairs).query(&mut con).unwrap();

            let key_refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
            group.bench_with_input(
                BenchmarkId::new("redis_mget", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        let _: Vec<Vec<u8>> = con.mget(black_box(&key_refs)).unwrap();
                    });
                },
            );
        }

        // Benchmark EagleDB MSET
        if eagle_available() {
            let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

            let pairs: Vec<(&[u8], &[u8])> = keys
                .iter()
                .zip(values.iter())
                .map(|(k, v)| (k.as_bytes(), v.as_slice()))
                .collect();

            group.bench_with_input(
                BenchmarkId::new("eagle_mset", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        client.mset(black_box(&pairs)).unwrap();
                    });
                },
            );

            // Pre-populate for MGET
            client.mset(&pairs).unwrap();

            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();
            group.bench_with_input(
                BenchmarkId::new("eagle_mget", batch_size),
                &batch_size,
                |b, _| {
                    b.iter(|| {
                        client.mget(black_box(&key_refs)).unwrap();
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark INCR operations (atomic counters)
fn bench_incr(c: &mut Criterion) {
    let mut group = c.benchmark_group("incr");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    // Benchmark Redis INCR
    if redis_available() {
        let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
        let mut con = client.get_connection().unwrap();

        // Initialize counter
        let _: () = con.set("bench:counter:redis", 0).unwrap();

        group.bench_function("redis_incr", |b| {
            b.iter(|| {
                let _: i64 = con.incr(black_box("bench:counter:redis"), 1).unwrap();
            });
        });
    }

    // Benchmark EagleDB INCR
    if eagle_available() {
        let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

        // Initialize counter
        client.set(b"bench:counter:eagle", b"0").unwrap();

        group.bench_function("eagle_incr", |b| {
            b.iter(|| {
                client.incr(black_box(b"bench:counter:eagle")).unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark Hash operations (HSET/HGET)
fn bench_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let value = random_value(256);

    // Benchmark Redis Hash operations
    if redis_available() {
        let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
        let mut con = client.get_connection().unwrap();

        group.bench_function("redis_hset", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let field = format!("field:{}", i);
                let _: () = con
                    .hset(black_box("bench:hash:redis"), &field, &value)
                    .unwrap();
                i += 1;
            });
        });

        // Pre-populate for HGET
        for i in 0..1000 {
            let field = format!("field:{}", i);
            let _: () = con.hset("bench:hash:redis", &field, &value).unwrap();
        }

        group.bench_function("redis_hget", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                let field = format!("field:{}", rng.random_range(0..1000));
                let _: Option<Vec<u8>> = con.hget(black_box("bench:hash:redis"), &field).unwrap();
            });
        });
    }

    // Benchmark EagleDB Hash operations
    if eagle_available() {
        let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

        group.bench_function("eagle_hset", |b| {
            let mut i = 0u64;
            b.iter(|| {
                let field = format!("field:{}", i);
                client
                    .hset(black_box(b"bench:hash:eagle"), field.as_bytes(), &value)
                    .unwrap();
                i += 1;
            });
        });

        // Pre-populate for HGET
        for i in 0..1000 {
            let field = format!("field:{}", i);
            client
                .hset(b"bench:hash:eagle", field.as_bytes(), &value)
                .unwrap();
        }

        group.bench_function("eagle_hget", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                let field = format!("field:{}", rng.random_range(0..1000));
                client
                    .hget(black_box(b"bench:hash:eagle"), field.as_bytes())
                    .unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark mixed workload (realistic usage patterns)
fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(15));

    // Pre-populate data
    let num_keys = 10000;
    let value_size = 256;
    let values: Vec<Vec<u8>> = (0..num_keys).map(|_| random_value(value_size)).collect();

    // Read-heavy workload (90% reads, 10% writes)
    group.throughput(Throughput::Elements(100));

    if redis_available() {
        let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
        let mut con = client.get_connection().unwrap();

        // Pre-populate
        for (i, value) in values.iter().enumerate() {
            let key = format!("bench:mixed:{}", i);
            let _: () = con.set(&key, value).unwrap();
        }

        group.bench_function("redis_read_heavy", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                for _ in 0..100 {
                    if rng.random_range(0..100) < 90 {
                        // Read
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        let _: Option<Vec<u8>> = con.get(&key).unwrap();
                    } else {
                        // Write
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        let _: () = con.set(&key, &values[0]).unwrap();
                    }
                }
            });
        });

        group.bench_function("redis_write_heavy", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                for _ in 0..100 {
                    if rng.random_range(0..100) < 30 {
                        // Read
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        let _: Option<Vec<u8>> = con.get(&key).unwrap();
                    } else {
                        // Write
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        let _: () = con.set(&key, &values[0]).unwrap();
                    }
                }
            });
        });

        group.bench_function("redis_balanced", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                for _ in 0..100 {
                    if rng.random_range(0..100) < 50 {
                        // Read
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        let _: Option<Vec<u8>> = con.get(&key).unwrap();
                    } else {
                        // Write
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        let _: () = con.set(&key, &values[0]).unwrap();
                    }
                }
            });
        });
    }

    if eagle_available() {
        let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

        // Pre-populate
        for (i, value) in values.iter().enumerate() {
            let key = format!("bench:mixed:{}", i);
            client.set(key.as_bytes(), value).unwrap();
        }

        group.bench_function("eagle_read_heavy", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                for _ in 0..100 {
                    if rng.random_range(0..100) < 90 {
                        // Read
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        client.get(key.as_bytes()).unwrap();
                    } else {
                        // Write
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        client.set(key.as_bytes(), &values[0]).unwrap();
                    }
                }
            });
        });

        group.bench_function("eagle_write_heavy", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                for _ in 0..100 {
                    if rng.random_range(0..100) < 30 {
                        // Read
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        client.get(key.as_bytes()).unwrap();
                    } else {
                        // Write
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        client.set(key.as_bytes(), &values[0]).unwrap();
                    }
                }
            });
        });

        group.bench_function("eagle_balanced", |b| {
            let mut rng = rand::rng();
            b.iter(|| {
                for _ in 0..100 {
                    if rng.random_range(0..100) < 50 {
                        // Read
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        client.get(key.as_bytes()).unwrap();
                    } else {
                        // Write
                        let key = format!("bench:mixed:{}", rng.random_range(0..num_keys));
                        client.set(key.as_bytes(), &values[0]).unwrap();
                    }
                }
            });
        });
    }

    group.finish();
}

/// Benchmark latency distribution
fn bench_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency");
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(1));

    let value = random_value(256);

    // Cold start latency (new connection + first operation)
    if redis_available() {
        group.bench_function("redis_cold_set", |b| {
            b.iter(|| {
                let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
                let mut con = client.get_connection().unwrap();
                let _: () = con.set("bench:cold", &value).unwrap();
            });
        });
    }

    if eagle_available() {
        group.bench_function("eagle_cold_set", |b| {
            b.iter(|| {
                let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();
                client.set(b"bench:cold", &value).unwrap();
            });
        });
    }

    // Warm latency (reusing connection)
    if redis_available() {
        let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
        let mut con = client.get_connection().unwrap();

        group.bench_function("redis_warm_set", |b| {
            b.iter(|| {
                let _: () = con.set("bench:warm", &value).unwrap();
            });
        });

        // Pre-populate
        let _: () = con.set("bench:warm", &value).unwrap();

        group.bench_function("redis_warm_get", |b| {
            b.iter(|| {
                let _: Vec<u8> = con.get("bench:warm").unwrap();
            });
        });
    }

    if eagle_available() {
        let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

        group.bench_function("eagle_warm_set", |b| {
            b.iter(|| {
                client.set(b"bench:warm", &value).unwrap();
            });
        });

        // Pre-populate
        client.set(b"bench:warm", &value).unwrap();

        group.bench_function("eagle_warm_get", |b| {
            b.iter(|| {
                client.get(b"bench:warm").unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark pipeline operations
fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");
    group.measurement_time(Duration::from_secs(10));

    let pipeline_sizes = [10, 50, 100, 500];
    let value = random_value(256);

    for size in pipeline_sizes {
        group.throughput(Throughput::Elements(size as u64));

        // Redis pipeline
        if redis_available() {
            let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
            let mut con = client.get_connection().unwrap();

            group.bench_with_input(
                BenchmarkId::new("redis_pipeline_set", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut pipe = redis::pipe();
                        for i in 0..size {
                            pipe.set(format!("bench:pipe:{}", i), &value);
                        }
                        let _: Vec<()> = pipe.query(&mut con).unwrap();
                    });
                },
            );

            // Pre-populate for GET pipeline
            for i in 0..size {
                let _: () = con.set(format!("bench:pipe:{}", i), &value).unwrap();
            }

            group.bench_with_input(
                BenchmarkId::new("redis_pipeline_get", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        let mut pipe = redis::pipe();
                        for i in 0..size {
                            pipe.get(format!("bench:pipe:{}", i));
                        }
                        let _: Vec<Vec<u8>> = pipe.query(&mut con).unwrap();
                    });
                },
            );
        }

        // EagleDB doesn't support pipelining in the same way,
        // but we can measure sequential operations for comparison
        if eagle_available() {
            let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

            group.bench_with_input(
                BenchmarkId::new("eagle_sequential_set", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        for i in 0..size {
                            let key = format!("bench:seq:{}", i);
                            client.set(key.as_bytes(), &value).unwrap();
                        }
                    });
                },
            );

            // Pre-populate for GET
            for i in 0..size {
                let key = format!("bench:seq:{}", i);
                client.set(key.as_bytes(), &value).unwrap();
            }

            group.bench_with_input(
                BenchmarkId::new("eagle_sequential_get", size),
                &size,
                |b, &size| {
                    b.iter(|| {
                        for i in 0..size {
                            let key = format!("bench:seq:{}", i);
                            client.get(key.as_bytes()).unwrap();
                        }
                    });
                },
            );
        }
    }

    group.finish();
}

/// Benchmark varying key sizes
fn bench_key_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_sizes");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_secs(10));

    let key_sizes = [16, 64, 256, 1024];
    let value = random_value(256);

    for key_size in key_sizes {
        let key: String = (0..key_size).map(|_| 'k').collect();

        if redis_available() {
            let client = redis::Client::open(format!("redis://{}/", REDIS_ADDR)).unwrap();
            let mut con = client.get_connection().unwrap();

            group.bench_with_input(
                BenchmarkId::new("redis_set_keysize", key_size),
                &key_size,
                |b, _| {
                    b.iter(|| {
                        let _: () = con.set(black_box(&key), &value).unwrap();
                    });
                },
            );

            // Pre-populate
            let _: () = con.set(&key, &value).unwrap();

            group.bench_with_input(
                BenchmarkId::new("redis_get_keysize", key_size),
                &key_size,
                |b, _| {
                    b.iter(|| {
                        let _: Vec<u8> = con.get(black_box(&key)).unwrap();
                    });
                },
            );
        }

        if eagle_available() {
            let mut client = EagleClient::connect(EAGLE_ADDR).unwrap();

            group.bench_with_input(
                BenchmarkId::new("eagle_set_keysize", key_size),
                &key_size,
                |b, _| {
                    b.iter(|| {
                        client.set(black_box(key.as_bytes()), &value).unwrap();
                    });
                },
            );

            // Pre-populate
            client.set(key.as_bytes(), &value).unwrap();

            group.bench_with_input(
                BenchmarkId::new("eagle_get_keysize", key_size),
                &key_size,
                |b, _| {
                    b.iter(|| {
                        client.get(black_box(key.as_bytes())).unwrap();
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_get_set,
    bench_mget_mset,
    bench_incr,
    bench_hash,
    bench_mixed_workload,
    bench_latency,
    bench_pipeline,
    bench_key_sizes,
);

criterion_main!(benches);
