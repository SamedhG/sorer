use sorer::parsers::Data;
use sorer::reader::*;
use std::env;
use std::fs::File;
use std::io::SeekFrom;
use std::io::{BufRead, BufReader, Seek};
use std::thread;

mod clap;
use clap::*;

fn main() {
    // parse the arguments
    let args: Vec<String> = env::args().collect();
    let parsed_args = ProgArgs::from(args);

    // infer the schema
    let f: File = File::open(parsed_args.file.clone()).unwrap();
    let reader = BufReader::new(f);
    let schema = infer_schema(reader);

    // if argument is `-print_col_type`, we only need to infer the schema, in all other cases
    // we parse the data into columnar format in memory.
    match parsed_args.option {
        Options::PrintColType(n) => {
            if n >= schema.len() as u64 {
                println!(
                    "Error: There are only {} fields in the schema",
                    schema.len()
                );
            } else {
                println!("{}", format!("{:?}", schema[n as usize]).to_uppercase());
            }
            return;
        }
        _ => (),
    }

    // number of threads to use
    let num_threads = 8;

    // the total number of bytes to read
    let num_chars = if parsed_args.len == std::u64::MAX {
        std::fs::metadata(parsed_args.file.clone()).unwrap().len() - parsed_args.from
    } else {
        parsed_args.len
    };

    // each thread will parse this many characters +- some number
    let step = (num_chars / 8) as u64;

    // setup the work array with the from / len for each thread
    // each element in the work array is a tuple of (starting index, number of byte for this thread)
    let f: File = File::open(parsed_args.file.clone()).unwrap();
    let mut reader2 = BufReader::new(f);
    let mut work: Vec<(u64, u64)> = Vec::with_capacity(num_threads);

    work.push((parsed_args.from, step));
    let mut so_far = parsed_args.from;
    let mut buffer = Vec::new();

    for i in 1..num_threads {
        so_far += step;
        // advance the reader to this threads starting index
        reader2.seek(SeekFrom::Start(so_far)).unwrap();
        reader2.read_until(b'\n', &mut buffer).unwrap();
        work.push((so_far, step));

        // TODO: explain methodology for this...
        work.get_mut(i - 1).unwrap().1 += buffer.len() as u64 + 1;
        buffer.clear();
    }

    // initialize the threads with their own BufReader
    let mut threads = Vec::new();
    for w in work {
        let new_schema = schema.clone();
        let f: File = File::open(parsed_args.file.clone()).unwrap();
        let mut r = BufReader::new(f);
        threads.push(thread::spawn(move || {
            read_file(new_schema, &mut r, w.0, w.1)
        }));
    }

    // initialize the resulting columnar data frame
    let mut parsed_data: Vec<Vec<Data>> = Vec::with_capacity(schema.len());
    for _ in 0..schema.len() {
        parsed_data.push(Vec::new());
    }

    // let all the threads finish then combine the parsed data into the columnar data frame
    for t in threads {
        let mut x = t.join().unwrap();
        for i in 0..schema.len() {
            parsed_data
                .get_mut(i)
                .unwrap()
                .append(x.get_mut(i).unwrap());
        }
    }

    // metadata about the parsed file
    let num_cols = parsed_data.len() as u64;
    let num_lines = if num_cols != 0 {
        parsed_data[0].len() as u64
    } else {
        0
    };

    // Retrieve and return the requested data
    match parsed_args.option {
        Options::PrintColIdx(n1, n2) => {
            if n1 >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    schema.len()
                );
            } else if n2 >= num_lines {
                println!("Error: Only {} lines were parsed", num_lines);
            } else {
                println!("{}", parsed_data[n1 as usize][n2 as usize]);
            }
        }
        Options::IsMissingIdx(n1, n2) => {
            if n1 >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    schema.len()
                );
            } else if n2 >= num_lines {
                println!("Error: Only {} lines were parsed", num_lines);
            } else {
                if parsed_data[n1 as usize][n2 as usize] == Data::Null {
                    println!("{}", 1);
                } else {
                    println!("{}", 0);
                }
            }
        }
        _ => unreachable!(),
    }
}
