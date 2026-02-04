use criterion::{Criterion, criterion_group, criterion_main};

fn memory_benchmark(_c: &mut Criterion) {
    // Add your memory benchmarks here
}

criterion_group!(benches, memory_benchmark);
criterion_main!(benches);
