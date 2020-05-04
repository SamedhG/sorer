//! A module for inferring `SoR` schemas.
use crate::dataframe::Data;
use crate::parsers::parse_line;
use deepsize::DeepSizeOf;
use easy_reader::EasyReader;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom};

/// A plain enumeration of the possible data types used in `SoR`, this one
/// without its accompanying value.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize, DeepSizeOf)]
pub enum DataType {
    /// Has the highest data type precedence.
    String,
    /// Has the second highest data type precedence.
    Float,
    /// Has the third highest data type precedence.
    Int,
    /// Has the fourth highest data type precedence.
    Bool,
}

// Get the dominant data type between two `DataType`s
fn get_dominant_data_type(
    cur_dominant_type: &DataType,
    other_type: &Data,
) -> DataType {
    match (cur_dominant_type, other_type) {
        (_, Data::String(_)) => DataType::String,
        (DataType::String, _) => DataType::String,
        (_, Data::Float(_)) => DataType::Float,
        (DataType::Float, _) => DataType::Float,
        (_, Data::Int(_)) => DataType::Int,
        (DataType::Int, _) => DataType::Int,
        _ => DataType::Bool,
    }
}

/// Infers the schema of the file with the given `file_name`.
/// Full information on how schema inference works can be found
/// [here](../index.html#schema-inference)
pub fn infer_schema(file_name: &str) -> Result<Vec<DataType>, io::Error> {
    infer_schema_for_n_lines(file_name, 300)
}

/// Infers the schema of the file opened by the given `reader`.
/// Full information on how schema inference works can be found
/// [here](../index.html#schema-inference)
pub(crate) fn infer_schema_for_n_lines(
    file_name: &str,
    num_lines_to_parse: usize,
) -> Result<Vec<DataType>, io::Error> {
    let book_end = num_lines_to_parse / 3;
    let mut parsed_lines = Vec::new();
    let mut reader = BufReader::new(File::open(file_name)?).split(b'\n');

    // infer the schema at the beginning
    let mut i = 0;
    while let Some(line) = reader.next() {
        handle_line_inference(&line?, &mut parsed_lines);
        i += 1;
        if i == book_end {
            break;
        }
    }

    // seek to middle and to infer the schema in the middle
    let mid_pt = fs::metadata(file_name)?.len() / 2;
    let mut f = File::open(file_name)?;
    f.seek(SeekFrom::Start(mid_pt))?;
    let mut reader = BufReader::new(f).split(b'\n');
    // throw away the first line since we started somewhere randomly in the
    // middle
    reader.next();
    i = 0;
    while let Some(line) = reader.next() {
        handle_line_inference(&line?, &mut parsed_lines);
        i += 1;
        if i == book_end {
            break;
        }
    }

    // parse the end of the file
    let mut backward_reader = EasyReader::new(File::open(file_name)?)?;
    backward_reader.eof();
    i = 0;
    while let Some(line) = backward_reader.prev_line()? {
        handle_line_inference(&line.as_bytes(), &mut parsed_lines);
        i += 1;
        if i == book_end {
            break;
        }
    }

    let cur_width = parsed_lines.get(0).unwrap_or_else(|| EMPTY).len();
    let mut schema = Vec::with_capacity(cur_width);
    for i in 0..cur_width {
        let mut data_type = DataType::Bool;
        for row in &parsed_lines {
            data_type = get_dominant_data_type(&data_type, &row[i]);
            if data_type == DataType::String {
                break;
            }
        }
        schema.push(data_type);
    }

    Ok(schema)
}

const EMPTY: &Vec<Data> = &Vec::new();

fn handle_line_inference(i: &[u8], current_lines: &mut Vec<Vec<Data>>) {
    if let Some(parsed) = parse_line(i) {
        match parsed
            .len()
            .cmp(&current_lines.get(0).unwrap_or_else(|| EMPTY).len())
        {
            Ordering::Greater => {
                current_lines.clear();
                current_lines.push(parsed);
            }
            Ordering::Equal => {
                current_lines.push(parsed);
            }
            Ordering::Less => (),
        }
    }
}
