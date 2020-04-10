use bytecount;
use sorer::{dataframe::SorTerator, schema};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn buff_byte_count(file_name: &str) -> usize {
    let mut buf_reader = BufReader::new(File::open(file_name).unwrap());
    let mut newlines = 0;

    loop {
        let bytes_read = buf_reader.fill_buf().unwrap();
        let len = bytes_read.len();
        if len == 0 {
            return newlines;
        };
        newlines += bytecount::count(bytes_read, b'\n');
        buf_reader.consume(len);
    }
}

// An example of using the `SorTerator` for chunking SoR files
fn main() {
    let args: Vec<String> = env::args().collect();
    let schema = schema::infer_schema(&args[1]);
    let total_newlines = buff_byte_count(&args[1]);
    let max_rows_per_chunk = total_newlines / 8;
    let mut sor_terator = SorTerator::new(&args[1], schema, max_rows_per_chunk);

    let mut i = 0;
    while let Some(_chunk) = sor_terator.next() {
        i += 1;
    }

    println!(
        "Total newlines: {}, number of chunks: {}",
        total_newlines, i
    );
}
