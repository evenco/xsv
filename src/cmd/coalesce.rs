use std::iter::once;

use csv;

use CliResult;
use config::{Config, Delimiter};
use select::SelectColumns;
use util;

static USAGE: &'static str = "
Coalsece multiple columns, selecting the first non-empty column.

Usage:
    xsv coalesce [options] [--] <selection> [<input>]
    xsv coalesce --help

coalesce options:
    --name <name>       Name the coalesced column, otherwise infers the
                           the name as the first header value.

Common options:
    -h, --help             Display this message
    -o, --output <file>    Write output to <file> instead of stdout.
    -n, --no-headers       When set, the first row will not be interpreted
                           as headers. (i.e., They are not searched, analyzed,
                           sliced, etc.)
    -d, --delimiter <arg>  The field delimiter for reading CSV data.
                           Must be a single character. (default: ,)
";

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_selection: SelectColumns,
	flag_name: Option<String>,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
}

macro_rules! coalesce {
    ($record:expr, $select:expr) => {
        $record.iter().chain($select.iter().map(|&i| &$record[i]).filter(|&f| f != b"").chain(once(&b""[..])).take(1))
    };
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
        match args.flag_name {
            None => wtr.write_record(coalesce!(&headers, &sel))?,
            Some(name) => wtr.write_record(headers.iter().chain(once(name.as_bytes())))?,
        };
    }
	
    let mut record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut record)? {
        wtr.write_record(coalesce!(&record, &sel))?;
    }
    wtr.flush()?;
    Ok(())
}
