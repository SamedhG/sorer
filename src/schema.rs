//! A module for inferring `SoR` schemas.
use crate::dataframe::Data;
use crate::parsers::{
    parse_delimited_bool, parse_delimited_float, parse_delimited_int,
    parse_delimited_null, parse_delimited_string,
};
use deepsize::DeepSizeOf;
use nom::branch::alt;
use nom::character::complete::multispace0;
use nom::multi::many0;
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader};

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

/// Get the dominant data type between two `DataType`s
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
pub fn infer_schema(file_name: &str) -> Vec<DataType> {
    let f: File = File::open(file_name).unwrap();
    let reader = BufReader::new(f);
    infer_schema_from_reader(reader)
}

/// Infers the schema of the file opened by the given `reader`.
/// Full information on how schema inference works can be found
/// [here](../index.html#schema-inference)
pub(crate) fn infer_schema_from_reader<T>(reader: T) -> Vec<DataType>
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

#[inline(always)]
fn parse_boxed_bool(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining, b) = parse_delimited_bool(i)?;
    Ok((remaining, Data::Bool(b)))
}

#[inline(always)]
fn parse_boxed_int(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining, i) = parse_delimited_int(i)?;
    Ok((remaining, Data::Int(i)))
}

#[inline(always)]
fn parse_boxed_float(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining, f) = parse_delimited_float(i)?;
    Ok((remaining, Data::Float(f)))
}

#[inline(always)]
fn parse_boxed_string(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining, s) = parse_delimited_string(i)?;
    Ok((remaining, Data::String(s)))
}

#[inline(always)]
fn parse_boxed_null(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining, _) = parse_delimited_null(i)?;
    Ok((remaining, Data::Null))
}

#[inline(always)]
fn parse_field(i: &[u8]) -> IResult<&[u8], Data> {
    alt((
        parse_boxed_null,
        parse_boxed_bool,
        parse_boxed_int,
        parse_boxed_float,
        parse_boxed_string,
    ))(i)
}

/// Parses a row of `SoR` data, `i` (as a `&[u8]`), into a `Option<Vec<Data>>`
/// Returning `Some` if `i` was a valid sor row, `None` otherwise. It parses
/// using the most conservative precedence possible. Types `bool`  are parsed
/// first, then `int`, then `float`, then `string`.
/// If a field is invalid, returns a `None`.
///
/// # Examples
/// ```
/// use sorer::parsers::parse_line;
/// use sorer::dataframe::Data;
/// let i = b"< 1 > < hi >< +2.2 >";
///
/// assert_eq!(Some(vec![Data::Bool(true),
///                  Data::String(String::from("hi")),
///                  Data::Float(2.2)]),
///            parse_line(i));
/// ```
///
/// # Safety
/// This function calls `std::str::from_utf8_unchecked`, meaning that it does not check that the
/// bytes passed to it are valid UTF-8. If this constraint is violated, undefined behavior results,
/// as the rest of Rust assumes that &strs are valid UTF-8.
///
/// Since `SoR` files are guaranteed to only contain valid C++ strings, and thus only valid `utf-8`,
/// then this constraint only applies to consumers of the crate and not users of the `SoRer`
/// executable.
pub fn parse_line(i: &[u8]) -> Option<Vec<Data>> {
    let (remaining_input, data) =
        many0(delimited(multispace0, parse_field, multispace0))(i).unwrap();
    if remaining_input != b"" {
        None
    } else {
        Some(data)
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
