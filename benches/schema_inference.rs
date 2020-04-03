use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sorer::schema;

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("schema inference", |b| {
        b.iter(|| schema::infer_schema(black_box("benches/schema.sor")))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
