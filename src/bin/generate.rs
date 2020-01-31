//! Generates a test file with random data in 8 columns 2 of each type.
//! can be run with `cargo run --bin generate`

extern crate rand;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use std::fs::File;
use std::io::prelude::*;

fn main() {
    let mut f = File::create("foo.txt").unwrap();
    let mut rng = thread_rng();
    for _ in 0..10000000 {
        let s1: String = rng.sample_iter(&Alphanumeric).take(12).collect();
        let s2: String = rng.sample_iter(&Alphanumeric).take(12).collect();
        let to_insert = format!(
            "< {} > < {} > < {} > < {} > < {} > < {} > < {} > < {} >\n",
            rng.gen::<i32>(),
            rng.gen_range(-100.0, 100.0),
            rng.gen::<i32>(),
            rng.gen_range(-100.0, 100.0),
            rng.gen_range::<i32, i32, i32>(0, 2),
            rng.gen_range::<i32, i32, i32>(0, 2),
            s1,
            s2
        );
        f.write_all(to_insert.as_bytes()).unwrap();
    }
}
