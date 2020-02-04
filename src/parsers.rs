//! A module for parsing raw byte slices into `SoR` data.

extern crate nom;
use std::str::from_utf8_unchecked;

use nom::branch::alt;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{digit1, multispace0};
use nom::combinator::{map, opt};
use nom::multi::many0;
use nom::number::complete::double;
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;

use crate::dataframe::Data;
use crate::schema::DataType;

#[inline(always)]
fn parse_bool(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining_input, b) = alt((tag("1"), tag("0")))(i)?;
    match b {
        b"1" => Ok((remaining_input, Data::Bool(true))),
        b"0" => Ok((remaining_input, Data::Bool(false))),
        _ => unreachable!(),
    }
}

#[inline(always)]
fn parse_delimited_bool(i: &[u8]) -> IResult<&[u8], Data> {
    delimited(
        terminated(tag("<"), multispace0),
        parse_bool,
        preceded(multispace0, tag(">")),
    )(i)
}

#[inline(always)]
fn parse_int(i: &[u8]) -> IResult<&[u8], Data> {
    let (remaining_input, (sign, number)) = tuple((opt(alt((tag("+"), tag("-")))), digit1))(i)?;
    let multiplier = match sign {
        None => 1,
        Some(b"+") => 1,
        Some(b"-") => -1,
        _ => unreachable!(),
    };
    // not unsafe because the spec guarantees only c++ characters in any field
    let num = unsafe { from_utf8_unchecked(number) }
        .parse::<i64>()
        .unwrap()
        * multiplier;
    Ok((remaining_input, Data::Int(num)))
}

#[inline(always)]
fn parse_delimited_int(i: &[u8]) -> IResult<&[u8], Data> {
    delimited(
        terminated(tag("<"), multispace0),
        parse_int,
        preceded(multispace0, tag(">")),
    )(i)
}

#[inline(always)]
fn parse_string(i: &[u8]) -> IResult<&[u8], Data> {
    // not unsafe because the spec guarantees only c++ characters in any field
    map(
        alt((delimited(tag("\""), is_not("\""), tag("\"")), is_not(" >"))),
        |s| Data::String(String::from(unsafe { from_utf8_unchecked(s) })),
    )(i)
}

#[inline(always)]
fn parse_delimited_string(i: &[u8]) -> IResult<&[u8], Data> {
    delimited(
        terminated(tag("<"), multispace0),
        parse_string,
        preceded(multispace0, tag(">")),
    )(i)
}

#[inline(always)]
fn parse_float(i: &[u8]) -> IResult<&[u8], Data> {
    map(double, Data::Float)(i)
}

#[inline(always)]
fn parse_delimited_float(i: &[u8]) -> IResult<&[u8], Data> {
    delimited(
        terminated(tag("<"), multispace0),
        parse_float,
        preceded(multispace0, tag(">")),
    )(i)
}

#[inline(always)]
fn parse_null(i: &[u8]) -> IResult<&[u8], Data> {
    map(multispace0, |_| Data::Null)(i)
}

#[inline(always)]
fn parse_delimited_null(i: &[u8]) -> IResult<&[u8], Data> {
    delimited(
        terminated(tag("<"), multispace0),
        parse_null,
        preceded(multispace0, tag(">")),
    )(i)
}

fn parse_field(i: &[u8]) -> IResult<&[u8], Data> {
    alt((
        parse_delimited_null,
        parse_delimited_bool,
        parse_delimited_int,
        parse_delimited_float,
        parse_delimited_string,
    ))(i)
}

/// Parses a row of `SoR` data, `i` (as a `&[u8]`), into a `Option<Vec<Data>>`
/// Returning `Some` if `i` was a valid sor row, `None` otherwise. It parses
/// using the most conservative precedence possible. Types `bool`  are parsed
/// first, then `int`, then `float`, then `string`.
/// If a field is invalid, returns a None.
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
    // note: multispace0 parses newline characters as well
    // so if we optimize the file reading need to change this
    let (remaining_input, data) =
        many0(delimited(multispace0, parse_field, multispace0))(i).unwrap();
    if remaining_input != b"" {
        None
    } else {
        Some(data)
    }
}

// NOTE: this is required since:
// the trait bound `&[u8]: nom::error::ParseError<&[u8]>` is not satisfied
// the trait `nom::error::ParseError<&[u8]>` is not implemented for `&[u8]`
// rustc(E0277) see: https://github.com/Geal/nom/issues/591
fn my_multispace(i: &[u8]) -> IResult<&[u8], &[u8]> {
    multispace0(i)
}

/// Parses a row of `SoR` data, `i` (as a `&[u8]`), into a `Option<Vec<Data>>`,
/// returning `Some` if the data types in `i` matches the `schema`. If the data
/// types match, but `i` contains fewer fields than `schema`, than
/// `Data::Null` is inserted. If the row has more fields than `schema`, then
/// the extra fields are discarded.
///
/// Further information on how parsing with [schemas](crate::reader::DataType) can
/// be found [here](../index.html#sor-fields) and
/// [here](../index.html#rows-that-dont-match-the-schema)
///
/// # Examples
/// ```
/// use sorer::schema::DataType;
/// use sorer::parsers::parse_line_with_schema;
/// use sorer::dataframe::Data;
///
/// let i = b"< 1 > < hi >";
/// let s = vec![DataType::Bool, DataType::String];
///
/// assert_eq!(Some(vec![Data::Bool(true),
///                  Data::String(String::from("hi"))]),
///            parse_line_with_schema(i, &s));
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
pub fn parse_line_with_schema(i: &[u8], schema: &Vec<DataType>) -> Option<Vec<Data>> {
    if i.is_empty() {
        return None;
    };
    let mut result: Vec<Data> = Vec::with_capacity(schema.len() + 1);
    let mut remaining_input = i;
    for column_type in schema {
        let (x, _) = my_multispace(remaining_input).unwrap();
        remaining_input = x;
        if remaining_input == b"" {
            result.push(Data::Null);
            continue;
        }
        match parse_delimited_null(remaining_input) {
            Ok((rem, d)) => {
                remaining_input = rem;
                result.push(d);
            }
            _ => match &column_type {
                DataType::String => match parse_delimited_string(remaining_input) {
                    Ok((x, d)) => {
                        result.push(d);
                        remaining_input = x;
                    }
                    _ => return None,
                },
                DataType::Float => match parse_delimited_float(remaining_input) {
                    Ok((x, d)) => {
                        result.push(d);
                        remaining_input = x;
                    }
                    _ => return None,
                },
                DataType::Int => match parse_delimited_int(remaining_input) {
                    Ok((x, d)) => {
                        result.push(d);
                        remaining_input = x;
                    }
                    _ => return None,
                },
                DataType::Bool => match parse_delimited_bool(remaining_input) {
                    Ok((x, d)) => {
                        result.push(d);
                        remaining_input = x;
                    }
                    _ => return None,
                },
            },
        }
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_string() {
        let x = parse_string(b"\"hello world\"");
        assert_eq!(x.unwrap().1, Data::String("hello world".to_string()));
        let x = parse_string(b"hello");
        assert_eq!(x.unwrap().1, Data::String("hello".to_string()));
        let x = parse_string(b"hello world");
        assert_eq!(x.unwrap().1, Data::String("hello".to_string()));
    }

    #[test]
    fn test_parse_bool() {
        let x = parse_bool(b"1");
        assert_eq!(x.unwrap().1, Data::Bool(true));
        let y = parse_bool(b"0");
        assert_eq!(y.unwrap().1, Data::Bool(false));
    }

    #[test]
    fn test_parse_int() {
        let x = parse_int(b"+123");
        assert_eq!(x.unwrap().1, Data::Int(123));
        let y = parse_int(b"-123");
        assert_eq!(y.unwrap().1, Data::Int(-123));
        let z = parse_int(b"123");
        assert_eq!(z.unwrap().1, Data::Int(123));
        let w = parse_int(b"01");
        assert_eq!(w.unwrap().1, Data::Int(1));
    }

    #[test]
    fn test_parse_float() {
        let x = parse_float(b"69E-01");
        assert_eq!(x.unwrap().1, Data::Float(6.9));
        let y = parse_float(b"-2.2");
        assert_eq!(y.unwrap().1, Data::Float(-2.2));
        let z = parse_float(b"2.2");
        assert_eq!(z.unwrap().1, Data::Float(2.2));
        let z = parse_float(b"4.20E+2");
        assert_eq!(z.unwrap().1, Data::Float(420.0));
    }

    #[test]
    fn test_parse_field() {
        let s = parse_field(b"< hello >");
        assert_eq!(s.unwrap().1, Data::String("hello".to_string()));
        let i = parse_field(b"<123>");
        assert_eq!(i.unwrap().1, Data::Int(123));
        let f = parse_field(b"< 123.123 >");
        assert_eq!(f.unwrap().1, Data::Float(123.123));
        let b = parse_field(b"< 1 >");
        assert_eq!(b.unwrap().1, Data::Bool(true));
        let n = parse_field(b"< >");
        assert_eq!(n.unwrap().1, Data::Null);
        let n2 = parse_field(b"<>");
        assert_eq!(n2.unwrap().1, Data::Null);
    }

    #[test]
    fn test_parse_line() {
        let line = parse_line(b"< hello > <123> <123.123> <> <1>");
        assert_eq!(
            line,
            Some(vec![
                Data::String("hello".to_string()),
                Data::Int(123),
                Data::Float(123.123),
                Data::Null,
                Data::Bool(true)
            ])
        );
        let line = parse_line(b"< hello > <123> <123.123> <> <1>");
        assert_eq!(
            line,
            Some(vec![
                Data::String("hello".to_string()),
                Data::Int(123),
                Data::Float(123.123),
                Data::Null,
                Data::Bool(true)
            ])
        );
        let empty = parse_line(b"");
        assert_eq!(empty, Some(vec![]));
        let i = parse_line(b"<123>");
        assert_eq!(i, Some(vec![Data::Int(123)]));
        let failing = parse_line(b"<1. 0>");
        assert_eq!(failing, None);
        let failing2 = parse_line(b"<bye world>");
        assert_eq!(failing2, None);
        let failing3 = parse_line(b"<+ 1>");
        assert_eq!(failing3, None);
    }

    #[test]
    fn test_parse_line_with_schema() {
        let schema = vec![
            DataType::String,
            DataType::Int,
            DataType::Float,
            DataType::String,
            DataType::Bool,
        ];
        let line = parse_line_with_schema(b" < hello > <123> <123.123> <> <1> ", &schema);
        assert_eq!(
            line,
            Some(vec![
                Data::String("hello".to_string()),
                Data::Int(123),
                Data::Float(123.123),
                Data::Null,
                Data::Bool(true)
            ])
        );

        let string_variants =
            parse_line_with_schema(b"< \"hi world\" > <+2> <1.1> <\"  hi \"> <0> ", &schema);
        assert_eq!(
            string_variants,
            Some(vec![
                Data::String("hi world".to_string()),
                Data::Int(2),
                Data::Float(1.1),
                Data::String("  hi ".to_string()),
                Data::Bool(false)
            ])
        );

        let string_variants2 =
            parse_line_with_schema(b"< \"<>\" > <-2> <1.19999> <<> <0> ", &schema);
        assert_eq!(
            string_variants2,
            Some(vec![
                Data::String("<>".to_string()),
                Data::Int(-2),
                Data::Float(1.19999),
                Data::String("<".to_string()),
                Data::Bool(false)
            ])
        );

        let parse_schema_precedence = parse_line_with_schema(b"<1> <1> <1.0> <1> <1>", &schema);
        assert_eq!(
            parse_schema_precedence,
            Some(vec![
                Data::String("1".to_string()),
                Data::Int(1),
                Data::Float(1.0),
                Data::String("1".to_string()),
                Data::Bool(true),
            ])
        );
    }

    #[test]
    fn test_parse_line_with_schema_and_missing_fields() {
        let schema = vec![DataType::String, DataType::Int, DataType::Float];

        let parse_explicit_missing = parse_line_with_schema(b"<> <-1> <>", &schema);
        assert_eq!(
            parse_explicit_missing,
            Some(vec![Data::Null, Data::Int(-1), Data::Null,])
        );

        let implicit_missing_at_end = parse_line_with_schema(b"<bye> <223> ", &schema);
        assert_eq!(
            implicit_missing_at_end,
            Some(vec![
                Data::String("bye".to_string()),
                Data::Int(223),
                Data::Null,
            ])
        );

        let too_many_fields_not_discarded =
            parse_line_with_schema(b"<bye> <223> <1.123> <> <1> <extra_field>", &schema);
        assert_eq!(
            too_many_fields_not_discarded,
            Some(vec![
                Data::String("bye".to_string()),
                Data::Int(223),
                Data::Float(1.123),
            ])
        );
    }

    #[test]
    fn test_parsing_bad_lines_with_schema() {
        let schema = vec![
            DataType::String,
            DataType::Int,
            DataType::Float,
            DataType::String,
            DataType::Bool,
        ];
        let bad_string =
            parse_line_with_schema(b"< hi world > <+2> <1.1> <\"  hi \"> <0> ", &schema);
        assert_eq!(bad_string, None);

        let bad_row_wrong_types = parse_line_with_schema(b"<world> <1.2> <123> <1> <0>", &schema);
        assert_eq!(bad_row_wrong_types, None);

        let empty = parse_line_with_schema(b"", &schema);
        assert_eq!(empty, None);
    }
}
