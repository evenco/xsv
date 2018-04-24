use std::collections::hash_map::{HashMap, Entry};

use csv;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Fill selected columns forwards in data.

This command fills empty fields in the selected column
using the last seen non-empty field in the CSV. This is
useful to forward-fill values which may only be included
the first time they are encountered.

Usage:
    xsv fill [options] [--] <selection> [<input>]
    xsv fill --help

fill options:
	-g --groupby <selection>     Group by specified columns.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

type ByteString = Vec<u8>;

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_selection: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
	flag_groupby: Option<SelectColumns>
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_selection);
	
	let wconfig = Config::new(&args.flag_output);
	
	if let Some(groupby) = args.flag_groupby {
		return fill_forward_groupby(rconfig, wconfig, groupby);
	}
	
    fill_forward_simple(rconfig, wconfig)
}

type Grouper = HashMap<Vec<ByteString>, HashMap<usize, ByteString>>;

fn fill_forward_groupby(rconfig: Config, wconfig: Config, groupby: SelectColumns) -> CliResult<()> {
	
	let mut rdr = rconfig.reader()?;
	let mut wtr = wconfig.writer()?;
	
    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;
	let gby = groupby.selection(&headers, true)?;
	
    if !rconfig.no_headers {
        rconfig.write_headers(&mut rdr, &mut wtr)?;
    }
	
	let mut grouper = Grouper::new();
	
	let mut record = csv::ByteRecord::new();
	
	while rdr.read_byte_record(&mut record)? {
		
		let groupkey : Vec<ByteString> = gby.iter().map(|&i| record[i].to_vec()).collect();
		
		// Set last valid value where applicable.
		for &i in sel.iter().filter(|&j| &record[*j] != b"") {
            match grouper.entry(groupkey.clone()) {
                Entry::Vacant(v) => {
                    let mut values = HashMap::new();
					values.insert(i, record[i].to_vec());
                    v.insert(values);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().insert(i, record[i].to_vec());
                }
            }
		}
		
		// Fill with last valid value, silently ignore when we haven't seen
		// a valid value
		let mut row : Vec<ByteString> = record.iter().map(|f| f.to_vec()).collect();
		for &i in sel.iter().filter(|&j| &record[*j] == b"") {
			match grouper.get(&groupkey) {
				None => {}
				Some(v) => {
					match v.get(&i) {
						Some(f) => { row[i] = f.clone() },
						None => {},
					}
				}
			}
		}
		wtr.write_record(row.iter())?;
	}
	wtr.flush()?;
	Ok(())
}

// This is the simplest of the fill methods, iterative, forward, and uncomplicated.
fn fill_forward_simple(rconfig: Config, wconfig: Config) -> CliResult<()> {
	
    let mut rdr = rconfig.reader()?;
    let mut wtr = wconfig.writer()?;

    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    if !rconfig.no_headers {
        rconfig.write_headers(&mut rdr, &mut wtr)?;
    }

	let mut lastvalid : Vec<Option<ByteString>> = Vec::new();
	{
		let mut record = csv::ByteRecord::new();
		rdr.read_byte_record(&mut record)?;
		lastvalid.extend(record.iter().map(|v| match v {
			b"" => None,
			value @ _ => Some(value.to_vec())
		}));
		wtr.write_record(&record)?;
	}

    for r in rdr.byte_records() {
		let mut record = r?;
		let mut riter = record.iter();
	
		for (i, field) in riter.enumerate() {
			let mut field = field;		
		
			if sel.contains(&i) {
				if field != b"" {
					lastvalid[i] = Some(field.to_vec());
				} else {
					field = match lastvalid[i] {
						None => b"",
						Some(ref value) => value
					}
				}
			}
			wtr.write_field(field)?;
		}
		wtr.write_record(None::<&[u8]>)?;
    }
    wtr.flush()?;
	Ok(())
}
	
