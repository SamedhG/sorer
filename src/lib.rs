//! # SoRer
//! `SoRer`, short for schema-on-read-er, is a program that can read files in
//! the SoR format and build columnar dataframes based on a dynamically
//! inferred schema.
//!
//! `SoRer` was built with speed and memory efficiency in mind so it can handle
//! processing files that are too large to fit into RAM.
//!
//! On our 2 year old desktop computer with a SATA SSD (meaning our testing is
//! likely near being bottlenecked by ssd read speeds) and 4 cores (4 threads),
//! `SoRer` can parse at ~`400 MB/s` on a large test file with 8 columns, two of
//! each data type with random values (which can be generated by running
//! `cargo run --release --bin generate` (warning don't do this inside of Docker,
//! you must install rust if you want to do this due to file i/o overhead when
//! using Docker). In a best case scenario, on a large file with 3 columns of
//! random bools, it can parse at over `700 MB/s`
//!
//! # Usage
//! ## Building SoRer
//! `SoRer` can be built on any computer by running the command: `make docker`
//! from the root of this repository. This builds a Docker image tagged as `sorer`.
//! It also builds the executable for `sorer`, located at
//! `/sorer/target/release/sorer` and copies over the executable to the current directory.
//!
//! Tests can be ran by running the command `make test`. The program can be ran
//! against a small test file named `sor.txt` by running the command: `make run`.
//!
//! Documentation can be built by running the command `make doc`. This builds the
//! documentation and copies it to `./doc/` on the host filesystem in this directory.
//! This documentation can be viewed by opening `./doc/sorer/index.html` in
//! your broswer.
//!
//! Note that ideally the best way to run our program is bare metal due to
//! overhead for using Docker (especially on Windows or Mac). You can do that by
//! installing `rust` by running the following command:
//!
//! `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
//!
//! Follow the printed instructions to source the cargo environment variables after
//! installing.
//!
//! Then build `sorer` by running `cargo build --release`. You may test the
//! program after installing `rust` by running `cargo test`. Documentation may
//! be built by running `cargo doc --no-deps --open`.
//!
//! ## Running SoRer
//! `SoRer` is ran as a command line tool that prints its results to `stdout`.
//!
//! The command line arguments are summarized in the below table
//!
//! | argument  | value type  | required?  | description  |
//! |:-:|:-:|---|---|
//! | -f   | \<string\>  | yes  | path to SoR file  |
//! | -from  | \<uint\>  | no  | starting position in file (in bytes)  |
//! | -len  | \<uint\>  |  no | number of bytes to read  |
//! | -print_col_type  | \<uint\>  | depends  | print the type of a column: BOOL, INT, FLOAT, STRING |
//! | -print_col_idx  | \<uint\> \<uint\>  | depends  | the first argument is the column, the second is the offset   |
//! | -is_missing_idx  | \<uint\> \<uint\>  | depends  | is there a missing field in the specified column offset  |
//!
//! When `<val>` in `-from <val>` is greater than 0, then the file is read
//! starting from the first complete line after `<val>`.
//!
//! When `<val>` in `-len <val>` is greater than 0, then the file is read
//! up until the last complete line.
//!
//! After running `make build`, running `make bash` will mount the current
//! the current directory to the docker container and start bash. If you
//! want to test any large files, you should do `make build` first, then copy
//! the files into this directory, then run `make bash`. Once you're in bash,
//! you can interact with `sorer` as usual:
//!
//!
//! # SoR Files
//! A SoR file is stored as plain text. Files consists of a sequence of rows,
//! each row must be separated by the newline character, "\n".
//! Each row is a sequence of fields, each field starting with "<" and ending
//! with ">". Spaces around delimiters are ignored.
//!
//! # SoR Fields
//!  A field can be either missing a value, or contain a value of one of four
//!  SoR types:
//! - `String`
//! - `Float`
//! - `Integer`
//! - `Bool`
//!
//! |Type   |Allowed values   |
//! |:-:|:-:|
//! | String  | Either as a sequences of characters without spaces or as a double quote delimited sequence of characters with spaces. Line breaks are not allowed in Strings. Can't be longer than 255 characters. Must be valid `utf-8` characters. |
//! | Float  | Any C++ float    |
//! | Integer  | Any C++ integer, ie a sequence of digits with an optional leading sign (must not be separated by whitespace)   |
//! |bool   | {1, 0}  |
//! | Missing (aka Null)  | must be empty, ie "<>"  |
//!
//!
//! ## Valid Examples of SoR Fields
//!
//! The following is an example of a row with four fields:
//!
//! `< 1 > < hi >< +2.2 >   < " bye ">`
//!
//! The following is an example of a row with explicit missing fields:
//!
//! `<1> <bye> <> <>`
//!
//! The following is also valid:
//!
//! `<> <> <> <>`
//!
//! ## Invalid Examples of SoR Fields
//!
//! ```c
//! <1. 2>       // space after dot
//!
//! <bye world>  // string with spaces and without quotes
//!
//! <+ 1>        // space after the +
//! ```
//!
//! NOTE: If a SoR file contains an invalid field, the row will be discarded
//! for both schema inference and data parsing.
//!
//! # Schema Inference
//! The schema that `SoRer` generates depends on the data types contained in
//! the row with the most number of fields in the first 500 rows (or
//! the whole file, whichever comes first), irregardless of
//! the `-from` command line argument. The data type chosen for
//! each column in the schema depends on the precedence of the data type.
//! Based on this data type precedence, a schema is inferred and then applied
//! to all fields in that column.
//!
//! The Data Type precedence is as follows:
//! 1. `String`
//! 2. `Float`
//! 3. `Integer`
//! 4. `Bool`
//!
//! This means that if any value is a `String`, the whole column is parsed
//! into a `String` type. Otherwise, if any of the values is a `Float`, then the
//!  column is of `Float` type. Otherwise, if you find a value with a sign or a
//! value larger than `1`, then the column is `Integer`. Otherwise the column
//!  is a `Bool` type.
//!
//! ## Rows that don't match the schema
//! If a row that doesn't match the schema is found after the schema is
//! inferred (meaning after the first 500 lines), then the row is discarded.
//! An example is if a schema is parsed as `<int> <int>`,  but a line coming
//! after the first 500 has `<string> <int>`, then it will be discarded.
//!
//! **Note** however, that it is valid for two rows in the same file to have a
//! different number of fields and still be considered to match the schema.
//! For rows with more fields than the schema, the extra fields will be
//! discarded but the row will still be parsed as long as the other fields
//! match the schema.
//!
//! E.g. The schema: `<int> <bool>` and a row: `<12> <0> <discarded>`
//! parses to `<12><0>`
//!
//!
//! If a row has less fields without explicit missing fields (i.e. "<>"), aka
//! implicit missing fields, `SoRer` will attempt to parse the fields
//! according to the schema and fill in explicit missing fields at the end
//! of the row until it matches the number of fields in the schema.
//!
//! E.g. The schema: `<int> <bool> <string>` and a row: `<12>`
//! parses to `<12><><>`

extern crate nom;

pub mod dataframe;
pub mod parsers;
pub mod schema;
