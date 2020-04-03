use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sorer::{parsers, schema};
use std::fs::File;
use std::io::{BufRead, BufReader};

fn read_line(file_name: &str) -> Vec<u8> {
    let f: File = File::open(file_name).unwrap();
    let mut reader = BufReader::new(f);
    let mut buffer = Vec::new();
    reader.read_until(b'\n', &mut buffer).unwrap();

    buffer
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("parsing thin row (4 columns) without schema", |b| {
        let line = read_line("benches/thin_row.sor");
        b.iter(|| parsers::parse_line(black_box(&line)))
    });

    c.bench_function("parsing medium row (8 columns) without schema", |b| {
        let line = read_line("benches/medium_row.sor");
        b.iter(|| parsers::parse_line(black_box(&line)))
    });

    c.bench_function("parsing wide row (32 columns) without schema", |b| {
        let line = read_line("benches/wide_row.sor");
        b.iter(|| parsers::parse_line(black_box(&line)))
    });

    c.bench_function("parsing thin row (4 columns) with schema", |b| {
        let line = read_line("benches/thin_row.sor");
        let schema = schema::infer_schema("benches/thin_row.sor");
        b.iter(|| parsers::parse_line_with_schema(black_box(&line), &schema))
    });

    c.bench_function("parsing medium row (8 columns) with schema", |b| {
        let line = read_line("benches/medium_row.sor");
        let schema = schema::infer_schema("benches/medium_row.sor");
        b.iter(|| parsers::parse_line_with_schema(black_box(&line), &schema))
    });

    c.bench_function("parsing wide row (32 columns) with schema", |b| {
        let line = read_line("benches/wide_row.sor");
        let schema = schema::infer_schema("benches/wide_row.sor");
        b.iter(|| parsers::parse_line_with_schema(black_box(&line), &schema))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
