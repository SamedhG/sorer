//! A module for inferring `SoR` schemas.
use crate::dataframe::Data;
use crate::parsers::parse_line;
use deepsize::DeepSizeOf;
use easy_reader::EasyReader;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

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
    infer_schema_for_n_lines(file_name, 500)
}

/// Infers the schema of the file opened by the given `reader`.
/// Full information on how schema inference works can be found
/// [here](../index.html#schema-inference)
pub(crate) fn infer_schema_for_n_lines(
    file_name: &str,
    num_lines_to_parse: usize,
) -> Result<Vec<DataType>, io::Error> {
    let book_end = 10;
    let mut parsed_lines = Vec::with_capacity(num_lines_to_parse);
    let reader = BufReader::new(File::open(file_name)?).split(b'\n');
    let mut rng = rand::thread_rng();
    let mut cur_width = 0;

    // parse the first 10 lines (if there are 5 lines, for example, then the
    // loop will only run 5 times)
    let first_book_end = reader.take(book_end);
    for line in first_book_end {
        handle_line_inference(&line?, &mut parsed_lines, &mut cur_width);
    }

    // generate num_lines - 20 indices as random lines to try to parse
    let reader = BufReader::new(File::open(file_name)?).split(b'\n');
    let rand_lines =
        reader.choose_multiple(&mut rng, num_lines_to_parse - book_end * 2);
    for rand_line in rand_lines {
        handle_line_inference(&rand_line?, &mut parsed_lines, &mut cur_width);
    }

    // parse the end of the file
    let mut backward_reader = EasyReader::new(File::open(file_name)?)?;
    backward_reader.eof();
    let mut num_rev_lines = 0;
    while let Some(line) = backward_reader.prev_line()? {
        handle_line_inference(
            &line.as_bytes(),
            &mut parsed_lines,
            &mut cur_width,
        );
        num_rev_lines += 1;
        if num_rev_lines == book_end {
            break;
        }
    }

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

fn handle_line_inference(
    i: &[u8],
    current_lines: &mut Vec<Vec<Data>>,
    cur_width: &mut usize,
) {
    if let Some(parsed) = parse_line(i) {
        match parsed.len().cmp(&cur_width) {
            Ordering::Greater => {
                *cur_width = parsed.len();
                current_lines.clear();
                current_lines.push(parsed);
            }
            Ordering::Equal => {
                *cur_width = parsed.len();
                current_lines.push(parsed);
            }
            Ordering::Less => (),
        }
    }
}

fn count_new_lines(file_name: &str) -> Result<usize, io::Error> {
    let mut buf_reader = BufReader::new(File::open(file_name)?);
    let mut new_lines = 0;

    loop {
        let bytes_read = buf_reader.fill_buf()?;
        let len = bytes_read.len();
        if len == 0 {
            return Ok(new_lines);
        };
        new_lines += bytecount::count(bytes_read, b'\n');
        buf_reader.consume(len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn infer_schema_test() {
        // Design decisions demonstrated by this test:
        // Null only columns are typed as a Bool
        let input = Cursor::new(b"<1><hello><>\n<12><1.2><>");
        let schema = infer_schema_from_reader(input);
        assert_eq!(
            schema,
            vec![DataType::Int, DataType::String, DataType::Bool]
        );

        let uses_row_w_most_fields =
            Cursor::new(b"<1>\n<hello><0>\n<1.1><0><2>");
        let schema2 = infer_schema_from_reader(uses_row_w_most_fields);
        assert_eq!(
            schema2,
            vec![DataType::Float, DataType::Bool, DataType::Int]
        );

        let type_precedence = Cursor::new(b"<0><3><3.3><str>\n<3><5.5><r><h>");
        let schema3 = infer_schema_from_reader(type_precedence);
        assert_eq!(
            schema3,
            vec![
                DataType::Int,
                DataType::Float,
                DataType::String,
                DataType::String
            ]
        );
    }
}
