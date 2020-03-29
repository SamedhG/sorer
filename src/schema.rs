//! A module for inferring `SoR` schemas.
use crate::dataframe::Data;
use crate::parsers::parse_line;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader};
/// A plain enumeration of the possible data types used in `SoR`, this one
/// without its accompanying value.
#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
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
pub fn infer_schema_from_file(file_name: String) -> Vec<DataType> {
    let f: File = File::open(file_name).unwrap();
    let reader = BufReader::new(f);
    infer_schema(reader)
}

/// Infers the schema of the file opened by the given `reader`.
/// Full information on how schema inference works can be found
/// [here](../index.html#schema-inference)
pub(crate) fn infer_schema<T>(reader: T) -> Vec<DataType>
where
    T: BufRead,
{
    let mut curr_length = 0;
    let mut parsed_lines = Vec::with_capacity(501);
    for (i, line) in reader.lines().enumerate() {
        if i == 500 {
            break;
        }
        let parsed = parse_line(line.unwrap().as_bytes());
        if parsed == None {
            continue;
        };
        let parsed = parsed.unwrap();
        match parsed.len().cmp(&curr_length) {
            Ordering::Greater => {
                parsed_lines.clear();
                curr_length = parsed.len();
                parsed_lines.push(parsed);
            }
            Ordering::Equal => parsed_lines.push(parsed),
            Ordering::Less => (),
        }
    }

    let mut schema = Vec::with_capacity(curr_length + 1);
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

        let uses_row_w_most_fields =
            Cursor::new(b"<1>\n<hello><0>\n<1.1><0><2>");
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
}
