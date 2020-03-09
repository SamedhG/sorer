use num_cpus;
use sorer::dataframe::*;
use sorer::schema::infer_schema_from_file;

use std::env;

mod clap;
use clap::*;

fn main() {
    // parse the arguments
    let args: Vec<String> = env::args().collect();
    let parsed_args = ProgArgs::from(args);

    let schema = infer_schema_from_file(parsed_args.file.clone());
    let num_threads = num_cpus::get();

    let dataframe = from_file(
        parsed_args.file,
        schema.clone(),
        parsed_args.from,
        parsed_args.len,
        num_threads,
    );

    // metadata about the parsed file
    let num_cols = dataframe.len();
    let num_lines = if num_cols != 0 {
        (match &dataframe[0] {
            Column::Bool(b) => b.len(),
            Column::Int(b) => b.len(),
            Column::Float(b) => b.len(),
            Column::String(b) => b.len(),
        })
    } else {
        0
    };

    // Retrieve and return the requested data
    match parsed_args.option {
        Options::PrintColIdx(n1, n2) => {
            if n1 >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    num_cols
                );
            } else if n2 >= num_lines {
                println!("Error: Only {} lines were parsed", num_lines);
            } else {
                println!("{}", get(&dataframe, n1, n2));
            }
        }
        Options::IsMissingIdx(n1, n2) => {
            if n1 >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    num_cols
                );
            } else if n2 >= num_lines {
                println!("Error: Only {} lines were parsed", num_lines);
            } else {
                if get(&dataframe, n1, n2) == Data::Null {
                    println!("{}", 1);
                } else {
                    println!("{}", 0);
                }
            }
        }
        Options::PrintColType(n) => {
            // note:
            // if argument is `-print_col_type`, we only need to infer the
            // schema, but we currently parse the file anyways so that
            // we dont disregard any -from and -len a arguments.
            // This can be very easily changed by adding a match right after
            // the call to `infer_schema` and returning from main if desired.
            if n >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    num_cols
                );
            } else {
                println!("{}", format!("{:?}", schema[n]).to_uppercase());
            }
        }
    }
}
