//! A module for inferring schemas on read and parsing very large files
//! into columnar data frames given a schema.

use std::io::{prelude::*, SeekFrom};

use crate::parsers::{parse_line, parse_line_with_schema, Data};

/// A plain enumeration of the possible data types used in `SoR`, this one
/// without its accompanying value.
#[derive(PartialEq, Debug, Clone)]
pub enum DataType {
    /// Has the highest data type precedence
    String,
    /// Has the second highest data type precedence
    Float,
    /// Has the third highest data type precedence
    Int,
    /// Has the fourth highest data type precedence
    Bool,
}

fn get_dominant_data_type(d1: &DataType, d2: &Data) -> DataType {
    match (d1, d2) {
        (_, Data::String(_)) => DataType::String,
        (DataType::String, _) => DataType::String,
        (_, Data::Float(_)) => DataType::Float,
        (DataType::Float, _) => DataType::Float,
        (_, Data::Int(_)) => DataType::Int,
        (DataType::Int, _) => DataType::Int,
        _ => DataType::Bool,
    }
}

/// Infers the schema of the file with the path from `options.file`.
/// Full information on how schema inference works can be found
/// [here](../index.html#schema-inference)
pub fn infer_schema<T>(reader: T) -> Vec<DataType>
where
    T: BufRead,
{
    let mut curr_length = 0;
    let mut parsed_lines = Vec::with_capacity(500);
    for (i, line) in reader.lines().enumerate() {
        if i == 500 {
            break;
        }
        let parsed = parse_line(line.unwrap().as_bytes());
        if parsed == None {
            continue;
        };
        let parsed = parsed.unwrap();
        if parsed.len() > curr_length {
            parsed_lines.clear();
            curr_length = parsed.len();
            parsed_lines.push(parsed);
        } else if parsed.len() == curr_length {
            parsed_lines.push(parsed);
        }
    }

    let mut schema = Vec::with_capacity(curr_length);
    for i in 0..curr_length {
        let mut data_type = DataType::Bool;
        for row in &parsed_lines {
            data_type = get_dominant_data_type(&data_type, &row[i]);
            if data_type == DataType::String {
                break;
            }
        }
        schema.push(data_type);
    }
    schema
}

/// Reads a file (even one too large to fit into memory) according to the given
/// `schema` and `options` and turns it into a columnar dataframe.
///
/// This is the top level function for using `SoRer` and the one you should be
///  using unless you are trying to extend `SoRer`. There are many intricate
/// facets to using `SoRer` so you *must* RTFM [here](../index.html)
pub fn read_file<T>(schema: Vec<DataType>, reader: &mut T, from: u64, len: u64) -> Vec<Vec<Data>>
where
    T: BufRead + Seek,
{
    reader.seek(SeekFrom::Start(from)).unwrap();
    let mut buffer = Vec::new();

    let mut so_far = if from != 0 {
        // throw away the first line
        let l1_len = reader.read_until(b'\n', &mut buffer).unwrap();
        buffer.clear();
        l1_len as u64
    } else {
        0
    };

    let mut parsed_data = Vec::with_capacity(schema.len());
    for _ in 0..schema.len() {
        parsed_data.push(Vec::new());
    }

    loop {
        let line_len = reader.read_until(b'\n', &mut buffer).unwrap();
        if line_len == 0 {
            break;
        }
        so_far += line_len as u64;
        if so_far >= len {
            break;
        }

        // parse line with schema and place into the columnar vec here
        match parse_line_with_schema(&buffer[..], &schema) {
            None => {
                buffer.clear();
                continue;
            }
            Some(data) => {
                data.iter()
                    .enumerate()
                    .for_each(|(i, d)| parsed_data.get_mut(i).unwrap().push(d.clone()));
            }
        }
        buffer.clear();
    }
    parsed_data
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
        let schema = infer_schema(input);
        assert_eq!(
            schema,
            vec![DataType::Int, DataType::String, DataType::Bool]
        );

        let uses_row_w_most_fields = Cursor::new(b"<1>\n<hello><0>\n<1.1><0><2>");
        let schema2 = infer_schema(uses_row_w_most_fields);
        assert_eq!(
            schema2,
            vec![DataType::Float, DataType::Bool, DataType::Int]
        );

        let type_precedence = Cursor::new(b"<0><3><3.3><str>\n<3><5.5><r><h>");
        let schema3 = infer_schema(type_precedence);
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

    #[test]
    fn test_read_file() {
        let schema = vec![DataType::String, DataType::Bool];

        let expected_col1 = vec![
            Data::String("1".to_string()),
            Data::String("a".to_string()),
            Data::String("1.2".to_string()),
        ];
        let expected_col2 = vec![Data::Bool(true), Data::Bool(false), Data::Null];
        let expected = vec![expected_col1, expected_col2];

        // Simple case : first nd last line are not discarded
        let mut input = Cursor::new(b"<1><1>\n<a><0>\n<1.2><>");
        let parsed1 = read_file(schema.clone(), &mut input, 0, 26);
        assert_eq!(parsed1, expected.clone());

        // last line is discarded
        let mut larger_input = Cursor::new(b"<1><1>\n<a><0>\n<1.2><>\n<no><1>");
        let parsed2 = read_file(schema.clone(), &mut larger_input, 0, 27);
        assert_eq!(parsed2, expected.clone());

        // first line is discarded
        let mut input_skipped_l1 = Cursor::new(b"<b><1>\n<1><1>\n<a><0>\n<1.2><>");
        let parsed3 = read_file(schema.clone(), &mut input_skipped_l1, 3, 26);
        assert_eq!(parsed3, expected.clone());

        // Invalid line is discarded
        // Note since parsed lines with schema is correctly tested we do not
        // need to test every possible way a line can be invalid here
        let mut input_with_invalid = Cursor::new(b"<1><1>\n<a><0>\n<c><1.2>\n<1.2><>");
        let parsed4 = read_file(schema.clone(), &mut input_with_invalid, 0, 32);
        assert_eq!(parsed4, expected.clone());
    }
}
