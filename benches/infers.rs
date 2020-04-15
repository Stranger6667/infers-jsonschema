use criterion::{black_box, criterion_group, criterion_main, Criterion};
use infers_jsonschema::inference::infer;
use serde_json::{from_str, json, Value};
use std::fs::File;
use std::io::Read;
use std::path::Path;

fn read_json(filepath: &str) -> Value {
    let path = Path::new(filepath);
    let mut file = File::open(&path).unwrap();
    let mut content = String::new();
    file.read_to_string(&mut content).ok().unwrap();
    let data: Value = from_str(&content).unwrap();
    data
}

fn canada_benchmark(c: &mut Criterion) {
    let data = black_box(read_json("benches/canada.json"));
    //    infer(&data);
    c.bench_function("canada bench", |b| b.iter(|| infer(&data)));
}

criterion_group!(benches, canada_benchmark);

criterion_main!(benches);
