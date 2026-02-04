use criterion::{Criterion, criterion_group, criterion_main};

fn pmem_benchmark(_c: &mut Criterion) {
    // Add your persistent memory benchmarks here
}

criterion_group!(benches, pmem_benchmark);
criterion_main!(benches);
