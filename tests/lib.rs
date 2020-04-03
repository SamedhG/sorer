use sorer::dataframe::*;
use sorer::schema::*;

#[test]
fn get_col_type() {
    let col_type_tests = vec![
        ("tests/0.sor", 0, DataType::Bool),
        ("tests/1.sor", 0, DataType::String),
        ("tests/2.sor", 0, DataType::Bool),
        ("tests/2.sor", 1, DataType::Int),
        ("tests/2.sor", 2, DataType::Float),
        ("tests/2.sor", 3, DataType::String),
        // commented out due to handins limitations on max submission size
        //        ("tests/3.sor", 4, DataType::Bool),
    ];

    for t in col_type_tests {
        let s = infer_schema(t.0);
        assert_eq!(*s.get(t.1).unwrap(), t.2);
    }
}

#[test]
fn is_missing_idx() {
    let is_missing_tests = vec![
        ("tests/0.sor", 0, 0, true),
        ("tests/1.sor", 0, 1, false),
        ("tests/2.sor", 1, 0, true),
        ("tests/2.sor", 1, 1, false),
    ];

    for t in is_missing_tests {
        let schema = infer_schema(t.0.clone());
        let data_frame = from_file(t.0, schema, 0, std::usize::MAX, 8);

        assert_eq!(get(&data_frame, t.1, t.2) == Data::Null, t.3);
    }

    // special case
    // ./sorer./sorer -f 1.sor -from 1 -len 74 -is_missing_idx 0 0
    let schema = infer_schema("tests/1.sor");
    let data_frame = from_file("tests/1.sor", schema, 1, 74, 8);

    assert_eq!(get(&data_frame, 0, 0) == Data::Null, false);
}

// NOTE: This test is ignored by default since running `cargo test` uses the debug build, which is
// much much slower than the release version (release is roughly 60x faster).
// If you want to run this test, run `cargo test --release -- --ignored`
#[test]
#[ignore]
fn print_col_idx() {
    let print_col_idx_tests = vec![
        ("tests/1.sor", 0, 3, Data::String("+1".to_string())),
        ("tests/2.sor", 3, 0, Data::String("hi".to_string())),
        ("tests/2.sor", 3, 1, Data::String("ho ho ho".to_string())),
        ("tests/2.sor", 2, 0, Data::Float(1.2)),
        ("tests/2.sor", 2, 1, Data::Float(-0.2)),
        // commented out due to handins limitations on max submission size
        //("tests/3.sor", 2, 10, Data::Float(1.2)),
        //("tests/3.sor", 2, 384200, Data::Float(1.2)),
        ("tests/4.sor", 0, 1, Data::Int(2147483647)),
        ("tests/4.sor", 0, 2, Data::Int(-2147483648)),
        ("tests/4.sor", 1, 1, Data::Float(-2e-09)),
        ("tests/4.sor", 1, 2, Data::Float(1e+10)),
    ];

    for t in print_col_idx_tests {
        let schema = infer_schema(t.0.clone());
        let data_frame = from_file(t.0, schema, 0, std::usize::MAX, 8);

        assert_eq!(get(&data_frame, t.1, t.2), t.3);
    }
    // special case:
    // ./sorer./sorer -f 1.sor -from 1 -len 74 -print_col_idx 0 6
    // "+2.2"
}
