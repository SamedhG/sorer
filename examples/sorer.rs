use num_cpus;
use sorer::dataframe::*;
use sorer::schema::infer_schema;
use std::env;

fn main() {
    // parse the arguments
    let args: Vec<String> = env::args().collect();
    let parsed_args = ProgArgs::from(args);

    match &parsed_args.option {
        Options::Help => {
            println!("{}", HELP_MSG);
            return;
        }
        _ => (),
    };

    let schema = infer_schema(&parsed_args.file.clone());
    match &parsed_args.option {
        Options::PrintColType(col_idx) => {
            if *col_idx >= schema.len() {
                println!(
                    "Error: There are only {} fields in the schema",
                    schema.len()
                );
            } else {
                println!(
                    "{}",
                    format!("{:?}", schema[*col_idx]).to_uppercase()
                );
            }
            return;
        }
        _ => (),
    };

    let num_threads = num_cpus::get();

    let dataframe = from_file(
        &parsed_args.file,
        schema.clone(),
        parsed_args.from,
        parsed_args.len,
        num_threads,
    );

    // metadata about the parsed file
    let num_cols = dataframe.len();
    let num_lines = if num_cols != 0 {
        match &dataframe[0] {
            Column::Bool(b) => b.len(),
            Column::Int(b) => b.len(),
            Column::Float(b) => b.len(),
            Column::String(b) => b.len(),
        }
    } else {
        0
    };

    // Retrieve and return the requested data
    match parsed_args.option {
        Options::PrintColIdx(col_idx, row_idx) => {
            if col_idx >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    num_cols
                );
            } else if row_idx >= num_lines {
                println!("Error: Only {} lines were parsed", num_lines);
            } else {
                println!("{}", get(&dataframe, col_idx, row_idx));
            }
        }
        Options::IsMissingIdx(col_idx, row_idx) => {
            if col_idx >= num_cols {
                println!(
                    "Error: There are only {} fields in the schema",
                    num_cols
                );
            } else if row_idx >= num_lines {
                println!("Error: Only {} lines were parsed", num_lines);
            } else if get(&dataframe, col_idx, row_idx) == Data::Null {
                println!("1");
            } else {
                println!("0");
            }
        }
        _ => unreachable!(),
    }
}

/// This command line program is a simple example of usage of the `sorer`
/// crate. It simply parses a file, answers one of three possible queries, and
/// exits. The point of this program is simply to be an example and nothing
/// more.
#[derive(Debug, Clone)]
pub(crate) struct ProgArgs {
    /// The name of the file to parse
    pub(crate) file: String,
    /// The starting byte offset in the file to begin parsing from
    pub(crate) from: usize,
    /// The ending byte offset in the file to stop parsing at
    pub(crate) len: usize,
    /// The query we will make about the parsed data
    pub(crate) option: Options,
}

// Enum to depict all the operations to be done on the binary file
#[derive(Debug, Clone, Copy)]
pub(crate) enum Options {
    /// Prints the data type of the column at the given column index
    PrintColType(usize),
    /// Prints the value at the given column, row index
    PrintColIdx(usize, usize),
    /// Prints "1" if the data at the given column, row index is 'missing' or
    /// "1" if it is present
    IsMissingIdx(usize, usize),
    /// If the user needed help
    Help,
}

// Parses command line arguments for this binary
impl From<Vec<String>> for ProgArgs {
    fn from(args: Vec<String>) -> Self {
        let mut file = None;
        let mut from = None;
        let mut len = None;
        let mut opt: Option<Options> = None;
        for mut i in 1..args.len() {
            if args[i] == "-f" {
                i += 1;
                match file {
                    None => file = Some(args[i].clone()),
                    Some(a) => panic!(format!("File was already set to {}", a)),
                }
            }
            if args[i] == "--from" {
                i += 1;
                match from {
                    None => from = Some(args[i].parse::<usize>().unwrap()),
                    Some(a) => panic!(format!("From was already set to {}", a)),
                }
            }
            if args[i] == "--len" {
                i += 1;
                match len {
                    None => len = Some(args[i].parse::<usize>().unwrap()),
                    Some(a) => panic!(format!("Len was already set to {}", a)),
                }
            }
            if args[i] == "--print-col-type" {
                match opt {
                    None => {
                        i += 1;
                        let n = args[i].parse::<usize>().unwrap();
                        opt = Some(Options::PrintColType(n));
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
            if args[i] == "--print-col-idx" {
                match opt {
                    None => {
                        i += 1;
                        let n1 = args[i].parse::<usize>().unwrap();
                        i += 1;
                        let n2 = args[i].parse::<usize>().unwrap();
                        opt = Some(Options::PrintColIdx(n1, n2));
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
            if args[i] == "--is_missing_idx" {
                match opt {
                    None => {
                        i += 1;
                        let n1 = args[i].parse::<usize>().unwrap();
                        i += 1;
                        let n2 = args[i].parse::<usize>().unwrap();
                        opt = Some(Options::IsMissingIdx(n1, n2));
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
            if args[i] == "--help" || args[i] == "-h" {
                match opt {
                    None => {
                        opt = Some(Options::Help);
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
        }
        match (&file, &from, &len, &opt) {
            (Some(file), Some(from), Some(len), Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: *from,
                len: *len,
                option: option.to_owned(),
            },
            (Some(file), None, Some(len), Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: 0,
                len: *len,
                option: option.to_owned(),
            },
            (Some(file), None, None, Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: 0,
                len: std::usize::MAX,
                option: option.to_owned(),
            },
            (Some(file), Some(from), None, Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: *from,
                len: std::usize::MAX,
                option: option.to_owned(),
            },
            (_, _, _, Some(option @ Options::Help)) => ProgArgs {
                file: "".to_string(),
                from: 0,
                len: std::usize::MAX,
                option: option.to_owned(),
            },
            _ => panic!("Missing required arguments"),
        }
    }
}

const HELP_MSG: &str = "sorer 1.0
Samedh G. & Thomas H.
Different types of queries to show information about what data was parsed

USAGE:
    sorer [OPTIONS] --file <file> <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -f, --file <file>    The name of the file to parse
        --from <from>    The starting byte offset in the file to begin parsing from [default: 0]
        --len <len>      The ending byte offset in the file to stop parsing at [default: 4294967295]

SUBCOMMANDS:
    --is-missing-idx <col-idx> <row-idx>   Prints '1' if the data at the given column, row index is 'missing' or '1' if it is present
    --print-col-idx <col-idx> <row-idx>    Prints the value at the given column, row index
    --print-col-type <col-idx>             Prints the data type of the column at the given column index
";
