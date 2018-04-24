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
}

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;

    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.arg_selection);

    let mut rdr = rconfig.reader()?;
    let mut wtr = Config::new(&args.flag_output).writer()?;

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
