//! This module defines functions to parse a `SoR` file into a columnar
//! format as a `Vec<Column>`.

use crate::parsers::parse_line_with_schema;
use crate::schema::DataType;
use deepsize::DeepSizeOf;
use serde::{Deserialize, Serialize};
use std::convert::{From, TryFrom};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Split};
use std::thread;

/// Represents a column of parsed data from a `SoR` file.
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize, DeepSizeOf)]
pub enum Column {
    /// A Column consisting of optional `i64`s.
    Int(Vec<Option<i64>>),
    /// A Column consisting of optional `bool`s.
    Bool(Vec<Option<bool>>),
    /// A Column consisting of optional `f64`s.
    Float(Vec<Option<f64>>),
    /// A Column consisting of optional `String`s.
    String(Vec<Option<String>>),
}

impl Column {
    pub fn len(&self) -> usize {
        match &self {
            &Column::Bool(col) => col.len(),
            &Column::Int(col) => col.len(),
            &Column::Float(col) => col.len(),
            &Column::String(col) => col.len(),
        }
    }
}

/// An enumeration of the possible `SoR` data types, that also contains the
/// data itself.
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum Data {
    /// A `String` cell.
    String(String),
    /// A `i64` cell.
    Int(i64),
    /// A `f64` cell.
    Float(f64),
    /// A `bool` cell.
    Bool(bool),
    /// A missing value.
    Null,
}

impl Data {
    /// Get the data assuming its a String
    pub fn unwrap_string(&self) -> String {
        match self {
            Data::String(s) => s.clone(),
            _ => panic!("unwrap error"),
        }
    }

    /// Get the data assuming its an int
    pub fn unwrap_int(&self) -> i64 {
        match self {
            Data::Int(n) => *n,
            _ => panic!("unwrap error"),
        }
    }

    /// Get the data assuming its a float
    pub fn unwrap_float(&self) -> f64 {
        match self {
            Data::Float(n) => *n,
            _ => panic!("unwrap error"),
        }
    }

    /// Get the data assuming its a bool
    pub fn unwrap_bool(&self) -> bool {
        match self {
            Data::Bool(n) => *n,
            _ => panic!("unwrap error"),
        }
    }
}

/// Generate a `Vec<Column>` matching the given schema.
fn init_columnar(schema: &[DataType]) -> Vec<Column> {
    let mut result = Vec::with_capacity(schema.len() + 1);
    for t in schema {
        match t {
            DataType::Bool => result.push(Column::Bool(Vec::new())),
            DataType::Int => result.push(Column::Int(Vec::new())),
            DataType::Float => result.push(Column::Float(Vec::new())),
            DataType::String => result.push(Column::String(Vec::new())),
        }
    }
    result
}

// TODO: this has a bug if num_threads is == 1. See tests/lib.rs
// `is_missing_idx` and `print_col_idx`
// TODO: use crossbeam for scoped thread spawning and change from_file to
// take `schema: &[DataType]`

/// Reads `len` number of bytes from a given file starting at the `from` byte
/// offset an according to the given `schema`.
///
/// This is the top level function for using `SoRer` and the one you should be
///  using unless you are trying to extend `SoRer`. There are many intricate
/// facets to using `SoRer` so you *must* RTFM [here](../index.html)
pub fn from_file(
    file_path: &str,
    schema: Vec<DataType>,
    from: usize,
    len: usize,
    num_threads: usize,
) -> Vec<Column> {
    // the total number of bytes to read
    let num_chars = if len == std::usize::MAX {
        (std::fs::metadata(file_path).unwrap().len() - from as u64) as f64
    } else {
        len as f64
    };
    // each thread will parse this many characters +- some number
    let step = (num_chars / num_threads as f64).ceil() as usize;

    // setup the work array with the from / len for each thread
    // each element in the work array is a tuple of (starting index, number of byte for this thread)
    let f: File = File::open(file_path).unwrap();
    let mut reader = BufReader::new(f);
    let mut work: Vec<(usize, usize)> = Vec::with_capacity(num_threads + 1);

    // add the first one separately since we want to access the previous thread's
    // work when in the loop. Since the work of the first thread will call
    // `read_file(schema, 0, step)` it will not throw away the first line
    // since from is 0 and will throw away the last line since step > 0
    work.push((from, step));

    let mut so_far = from;
    let mut buffer = Vec::new();

    // This loop finds the byte offset for the start of a line
    // by adding the length of the last line that a previous thread would've
    // thrown away. The work gets added to the following thread so that
    // each thread starts at a full line and reads only until the end of a line
    for i in 1..num_threads {
        so_far += step;
        // advance the reader to this threads starting index then
        // find the next newline character
        reader.seek(SeekFrom::Start(so_far as u64)).unwrap();
        reader.read_until(b'\n', &mut buffer).unwrap();
        work.push((so_far, step));

        // Since the previous thread throws away the last line, add the length
        // of the last line of prev thread to the work of this thread so that
        // we read all lines.
        work.get_mut(i - 1).unwrap().1 += buffer.len() as usize + 1;
        buffer.clear();
    }

    // initialize the threads with their own BufReader
    let mut threads = Vec::new();
    for w in work {
        let new_schema = schema.clone();
        let f: File = File::open(file_path.clone()).unwrap();
        let mut r = BufReader::new(f);
        // spawn the thread and give it a closure which calls `from_file`
        // to parse the data into columnar format.
        threads.push(thread::spawn(move || {
            read_chunk(new_schema, &mut r, w.0, w.1)
        }));
    }

    // initialize the resulting columnar data frame
    let mut parsed_data: Vec<Column> = init_columnar(&schema);
    // let all the threads finish then combine the parsed data into the
    // columnar data frame
    for t in threads {
        let mut x: Vec<Column> = t.join().unwrap();
        let iter = parsed_data.iter_mut().zip(x.iter_mut());
        for (complete, partial) in iter {
            match (complete, partial) {
                (Column::Bool(c1), Column::Bool(c2)) => c1.append(c2),
                (Column::Int(c1), Column::Int(c2)) => c1.append(c2),
                (Column::Float(c1), Column::Float(c2)) => c1.append(c2),
                (Column::String(c1), Column::String(c2)) => c1.append(c2),
                _ => panic!("Unexpected result from thread"),
            }
        }
    }

    parsed_data
}

/// Get the (i,j) element from the DataFrame
pub fn get(d: &[Column], col_idx: usize, row_idx: usize) -> Data {
    match &d[col_idx] {
        Column::Bool(b) => {
            if let Some(val) = &b[row_idx] {
                Data::Bool(*val)
            } else {
                Data::Null
            }
        }
        Column::Int(b) => {
            if let Some(val) = &b[row_idx] {
                Data::Int(*val)
            } else {
                Data::Null
            }
        }
        Column::Float(b) => {
            if let Some(val) = &b[row_idx] {
                Data::Float(*val)
            } else {
                Data::Null
            }
        }
        Column::String(b) => {
            if let Some(val) = &b[row_idx] {
                Data::String(val.clone())
            } else {
                Data::Null
            }
        }
    }
}

/// A helper function to help with multi-threading in the top level `from_file`
/// function. Does the heavy lifting of actually calling
/// [parser functions](::crate::parsers). Parsers a chunk of the given `reader`
/// up to `len` bytes starting at the `from` byte offset.
fn read_chunk<T>(
    schema: Vec<DataType>,
    reader: &mut T,
    from: usize,
    len: usize,
) -> Vec<Column>
where
    T: BufRead + Seek,
{
    reader.seek(SeekFrom::Start(from as u64)).unwrap();
    let mut buffer = Vec::new();

    let mut so_far = if from != 0 {
        // throw away the first line
        let l1_len = reader.read_until(b'\n', &mut buffer).unwrap();
        buffer.clear();
        l1_len
    } else {
        0
    };

    let mut parsed_data = init_columnar(&schema);

    loop {
        let line_len = reader.read_until(b'\n', &mut buffer).unwrap();
        so_far += line_len;
        if line_len == 0 || so_far >= len {
            break;
        }

        // parse line with schema and place into the columnar vec here
        match parse_line_with_schema(&buffer[..], &schema) {
            None => {
                buffer.clear();
                continue;
            }
            Some(data) => {
                let iter = data.iter().zip(parsed_data.iter_mut());
                for (d, col) in iter {
                    match (d, col) {
                        (Data::Bool(b), Column::Bool(c)) => c.push(Some(*b)),
                        (Data::Int(i), Column::Int(c)) => c.push(Some(*i)),
                        (Data::Float(f), Column::Float(c)) => c.push(Some(*f)),
                        (Data::String(s), Column::String(c)) => {
                            c.push(Some(s.clone()))
                        }
                        (Data::Null, Column::Bool(c)) => c.push(None),
                        (Data::Null, Column::Int(c)) => c.push(None),
                        (Data::Null, Column::Float(c)) => c.push(None),
                        (Data::Null, Column::String(c)) => c.push(None),
                        _ => panic!("Parser Failed"),
                    }
                }
            }
        }
        buffer.clear();
    }
    parsed_data
}

/// Used for chunking `SoR` files.
pub struct SorTerator {
    buf_reader: Split<BufReader<File>>,
    chunk_size: usize,
    schema: Vec<DataType>,
    empty_col: Column,
}

/// A chunking iterator that can chunk `SoR` files into `Vec<Column>`s where
/// all columns have the same length (number of rows) based an argument in
/// the constructor. The last element returned by `next` may have less than
/// `chunk_size` number of rows and it is up to the caller to verify the
/// length if needed.
impl SorTerator {
    /// Creates a new [`SorTerator`](::crate::dataframe::SorTerator)
    pub fn new(
        file_name: &str,
        schema: Vec<DataType>,
        chunk_size: usize,
    ) -> Self {
        SorTerator {
            buf_reader: BufReader::new(File::open(file_name).unwrap())
                .split(b'\n'),
            empty_col: Column::Bool(Vec::new()),
            chunk_size,
            schema,
        }
    }
}

/// Implementation for an `Iterator` that chunks a `SoR` file
impl Iterator for SorTerator {
    type Item = Vec<Column>;

    /// Advances this iterator until `self.chunk_size` rows have been parsed,
    /// returning `Some(Vec<Column>)` of the parsed rows when done, or `None`
    /// or the file has been completely parsed. The last element returned by
    /// `next` may have less than `chunk_size` number of rows and it is up to
    /// the caller to verify the length if needed.
    fn next(&mut self) -> Option<Self::Item> {
        let mut parsed_data = init_columnar(&self.schema);
        while let Some(Ok(line)) = self.buf_reader.next() {
            match parse_line_with_schema(&line, &self.schema) {
                None => continue,
                Some(data) => {
                    let iter = data.iter().zip(parsed_data.iter_mut());
                    for (d, col) in iter {
                        match (d, col) {
                            (Data::Bool(b), Column::Bool(c)) => {
                                c.push(Some(*b))
                            }
                            (Data::Int(i), Column::Int(c)) => c.push(Some(*i)),
                            (Data::Float(f), Column::Float(c)) => {
                                c.push(Some(*f))
                            }
                            (Data::String(s), Column::String(c)) => {
                                c.push(Some(s.clone()))
                            }
                            (Data::Null, Column::Bool(c)) => c.push(None),
                            (Data::Null, Column::Int(c)) => c.push(None),
                            (Data::Null, Column::Float(c)) => c.push(None),
                            (Data::Null, Column::String(c)) => c.push(None),
                            _ => panic!("Parser Failed"),
                        }
                    }
                }
            }
            if let Some(column) = parsed_data.get(0) {
                if column.len() == self.chunk_size {
                    return Some(parsed_data);
                }
            }
        }
        if parsed_data.get(0).unwrap_or(&self.empty_col).len() > 0 {
            Some(parsed_data)
        } else {
            None
        }
    }
}

impl From<Vec<Option<bool>>> for Column {
    fn from(v: Vec<Option<bool>>) -> Column {
        Column::Bool(v)
    }
}

impl From<Vec<Option<i64>>> for Column {
    fn from(v: Vec<Option<i64>>) -> Column {
        Column::Int(v)
    }
}

impl From<Vec<Option<f64>>> for Column {
    fn from(v: Vec<Option<f64>>) -> Column {
        Column::Float(v)
    }
}

impl From<Vec<Option<String>>> for Column {
    fn from(v: Vec<Option<String>>) -> Column {
        Column::String(v)
    }
}

impl TryFrom<Column> for Vec<Option<bool>> {
    type Error = &'static str;

    fn try_from(c: Column) -> Result<Self, Self::Error> {
        match c {
            Column::Bool(col) => Ok(col),
            _ => Err("The given column was not of type bool"),
        }
    }
}

impl TryFrom<Column> for Vec<Option<i64>> {
    type Error = &'static str;

    fn try_from(c: Column) -> Result<Self, Self::Error> {
        match c {
            Column::Int(col) => Ok(col),
            _ => Err("The given column was not of type int"),
        }
    }
}

impl TryFrom<Column> for Vec<Option<f64>> {
    type Error = &'static str;

    fn try_from(c: Column) -> Result<Self, Self::Error> {
        match c {
            Column::Float(col) => Ok(col),
            _ => Err("The given column was not of type float"),
        }
    }
}

impl TryFrom<Column> for Vec<Option<String>> {
    type Error = &'static str;

    fn try_from(c: Column) -> Result<Self, Self::Error> {
        match c {
            Column::String(col) => Ok(col),
            _ => Err("The given column was not of type String"),
        }
    }
}

/// Print the `Data` of a `Data` cell.
/// The number for `Int`s and `float`s.
/// 0 for `false`.
/// 1 for `true`.
/// A double quote delimited `String`.
/// and "Missing Value" for missing data.
impl fmt::Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Data::String(s) => write!(f, "\"{}\"", s),
            Data::Int(n) => write!(f, "{}", n),
            Data::Float(fl) => write!(f, "{}", fl),
            Data::Bool(true) => write!(f, "1"),
            Data::Bool(false) => write!(f, "0"),
            Data::Null => write!(f, "Missing Value"),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_file() {
        let schema = vec![DataType::String, DataType::Bool];

        let expected_col1 = Column::String(vec![
            Some("1".to_string()),
            Some("a".to_string()),
            Some("1.2".to_string()),
        ]);
        let expected_col2 = Column::Bool(vec![Some(true), Some(false), None]);
        let expected = vec![expected_col1, expected_col2];

        // Simple case : first nd last line are not discarded
        let mut input = Cursor::new(b"<1><1>\n<a><0>\n<1.2><>");
        let parsed1: Vec<Column> =
            read_chunk(schema.clone(), &mut input, 0, 26);
        assert_eq!(parsed1, expected.clone());

        // last line is discarded
        let mut larger_input = Cursor::new(b"<1><1>\n<a><0>\n<1.2><>\n<no><1>");
        let parsed2: Vec<Column> =
            read_chunk(schema.clone(), &mut larger_input, 0, 27);
        assert_eq!(parsed2, expected.clone());

        // first line is discarded
        let mut input_skipped_l1 =
            Cursor::new(b"<b><1>\n<1><1>\n<a><0>\n<1.2><>");
        let parsed3: Vec<Column> =
            read_chunk(schema.clone(), &mut input_skipped_l1, 3, 26);
        assert_eq!(parsed3, expected.clone());

        // Invalid line is discarded
        // Note since parsed lines with schema is correctly tested we do not
        // need to test every possible way a line can be invalid here
        let mut input_with_invalid =
            Cursor::new(b"<1><1>\n<a><0>\n<c><1.2>\n<1.2><>");
        let parsed4: Vec<Column> =
            read_chunk(schema.clone(), &mut input_with_invalid, 0, 32);
        assert_eq!(parsed4, expected.clone());
    }

    #[test]
    fn test_sor_terator() {
        let schema = vec![
            DataType::Bool,
            DataType::Int,
            DataType::Float,
            DataType::String,
        ];
        let mut sor_terator =
            SorTerator::new("tests/sor_terator.sor", schema, 10);
        let mut chunk = sor_terator.next();
        assert_eq!(chunk.unwrap().get(0).unwrap().len(), 10);
        chunk = sor_terator.next();
        assert_eq!(chunk.unwrap().get(0).unwrap().len(), 5);
        chunk = sor_terator.next();
        assert!(chunk.is_none());
    }
}
