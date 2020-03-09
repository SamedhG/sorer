// Struct containing the data from the command line arguments
#[derive(Debug, Clone)]
pub(crate) struct ProgArgs {
    pub(crate) file: String,
    pub(crate) from: usize,
    pub(crate) len: usize,
    pub(crate) option: Options,
}

// Enum to depict all the operations to be done on the binary file
#[derive(Debug, Clone, Copy)]
pub(crate) enum Options {
    PrintColType(usize),
    PrintColIdx(usize, usize),
    IsMissingIdx(usize, usize),
}

// Parses command line arguments for this binary
impl From<Vec<String>> for ProgArgs {
    fn from(args: Vec<String>) -> Self {
        let mut file = None;
        let mut from = None;
        let mut len = None;
        let mut opt: Option<Options> = None;
        for mut i in 1..args.len() {
            if args[i] == "-f" {
                i += 1;
                match file {
                    None => file = Some(args[i].clone()),
                    Some(a) => panic!(format!("File was already set to {}", a)),
                }
            }
            if args[i] == "-from" {
                i += 1;
                match from {
                    None => from = Some(args[i].parse::<usize>().unwrap()),
                    Some(a) => panic!(format!("From was already set to {}", a)),
                }
            }
            if args[i] == "-len" {
                i += 1;
                match len {
                    None => len = Some(args[i].parse::<usize>().unwrap()),
                    Some(a) => panic!(format!("Len was already set to {}", a)),
                }
            }
            if args[i] == "-print_col_type" {
                match opt {
                    None => {
                        i += 1;
                        let n = args[i].parse::<usize>().unwrap();
                        opt = Some(Options::PrintColType(n));
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
            if args[i] == "-print_col_idx" {
                match opt {
                    None => {
                        i += 1;
                        let n1 = args[i].parse::<usize>().unwrap();
                        i += 1;
                        let n2 = args[i].parse::<usize>().unwrap();
                        opt = Some(Options::PrintColIdx(n1, n2));
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
            if args[i] == "-is_missing_idx" {
                match opt {
                    None => {
                        i += 1;
                        let n1 = args[i].parse::<usize>().unwrap();
                        i += 1;
                        let n2 = args[i].parse::<usize>().unwrap();
                        opt = Some(Options::IsMissingIdx(n1, n2));
                    }
                    Some(a) => {
                        panic!(format!("Option was already set to {:?}", a))
                    }
                }
            }
        }
        match (&file, &from, &len, &opt) {
            (Some(file), Some(from), Some(len), Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: *from,
                len: *len,
                option: option.to_owned(),
            },
            (Some(file), None, Some(len), Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: 0,
                len: *len,
                option: option.to_owned(),
            },
            (Some(file), None, None, Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: 0,
                len: std::usize::MAX,
                option: option.to_owned(),
            },
            (Some(file), Some(from), None, Some(option)) => ProgArgs {
                file: file.to_owned(),
                from: *from,
                len: std::usize::MAX,
                option: option.to_owned(),
            },
            _ => panic!("Missing required arguments"),
        }
    }
}
