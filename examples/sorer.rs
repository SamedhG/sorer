use clap::Clap;
use num_cpus;
use sorer::dataframe::*;
use sorer::schema::infer_schema;

/// This command line program is a simple example of usage of the `sorer`
/// crate. It simply parses a file, answers one of three possible queries, and
/// exits. The point of this program is simply to be an example and nothing
/// more.
#[derive(Clap)]
#[clap(version = "1.0", author = "Samedh G. & Thomas H.")]
struct ProgArgs {
    /// The name of the file to parse
    #[clap(short = "f", long = "file")]
    file: String,
    /// The starting byte offset in the file to begin parsing from
    #[clap(long = "from", default_value = "0")]
    from: usize,
    /// The ending byte offset in the file to stop parsing at
    #[clap(long = "len", default_value = "4294967295")]
    len: usize,
    /// The query we will make about the parsed data
    #[clap(subcommand)]
    query: Query,
}

/// Different types of queries to show information about what data was parsed
#[derive(Clap)]
enum Query {
    /// Prints the data type of the column at the given column index
    PrintColType { col_idx: usize },
    /// Prints the value at the given column, row index
    PrintColIdx { col_idx: usize, row_idx: usize },
    /// Prints "1" if the data at the given column, row index is 'missing' or
    /// "1" if it is present
    IsMissingIdx { col_idx: usize, row_idx: usize },
}

fn main() {
    // parse the arguments
    let parsed_args = ProgArgs::parse();

    let schema = infer_schema(&parsed_args.file.clone());
    match &parsed_args.query {
        Query::PrintColType { col_idx } => {
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
    match parsed_args.query {
        Query::PrintColIdx { col_idx, row_idx } => {
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
        Query::IsMissingIdx { col_idx, row_idx } => {
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
