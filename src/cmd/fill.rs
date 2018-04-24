use std::collections::hash_map::{HashMap, Entry};
use std::io;

use csv;

use CliResult;
use config::{Config, Delimiter};
use select::{SelectColumns, Selection};
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

type BoxedWriter = csv::Writer<Box<io::Write+'static>>;
type BoxedReader = csv::Reader<Box<io::Read+'static>>;

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
	
	let mut rdr = rconfig.reader()?;
	let mut wtr = wconfig.writer()?;
	
    let headers = rdr.byte_headers()?.clone();
    let select = rconfig.selection(&headers)?;
	let groupby = match args.flag_groupby {
		Some(value) => Some(value.selection(&headers, !rconfig.no_headers)?),
		None => None,
	};
	
    if !rconfig.no_headers {
        rconfig.write_headers(&mut rdr, &mut wtr)?;
    }
	let ffill = ForwardFill::new(groupby, select);
	ffill.fill(&mut rdr, &mut wtr)
}

type Grouper = HashMap<Vec<ByteString>, HashMap<usize, ByteString>>;
type VecRecord = Vec<ByteString>;

trait Filler {
	
	fn groupbykey(&self, groupkey: Option<VecRecord>, record: &csv::ByteRecord, groupby: Option<&Selection>) -> Result<VecRecord, String> {
		match groupkey {
			Some(value) => Ok(value),
			None => Ok(match groupby {
				Some(ref value) => value.iter().map(|&i| record[i].to_vec()).collect(),
				None => vec![]
			})
		}
	}
	
	fn fill(self, rdr: &mut BoxedReader, wtr: &mut BoxedWriter) -> CliResult<()>;
	fn memorize(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> CliResult<()>;
	fn filledvalues(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> Result<VecRecord, String>;
}

struct ForwardFill {
	grouper : Grouper,
	groupby : Option<Selection>,
	select : Selection,
}

impl ForwardFill {
	
	fn new(groupby: Option<Selection>, select: Selection) -> Self {
		ForwardFill {
			grouper: Grouper::new(),
			groupby: groupby,
			select: select
		}
	}
	
}

impl Filler for ForwardFill {
	
	fn fill(mut self, rdr: &mut BoxedReader, wtr: &mut BoxedWriter) -> CliResult<()> {
		let mut record = csv::ByteRecord::new();
	
		while rdr.read_byte_record(&mut record)? {
			
			// Precompute groupby key
			let groupbykey = Some(self.groupbykey(None, &record, self.groupby.as_ref())?);
			self.memorize(&record, groupbykey.clone())?;
			
			let row = self.filledvalues(&record, groupbykey.clone())?;
			wtr.write_record(row.iter())?;
		}
		wtr.flush()?;
		Ok(())
	}
	
	fn filledvalues(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> Result<VecRecord, String> {
		let groupkey = self.groupbykey(groupkey, record, self.groupby.as_ref())?;

		let mut row : VecRecord = record.iter().map(|f| f.to_vec()).collect();
		for &i in self.select.iter().filter(|&j| &record[*j] == b"") {
			match self.grouper.get(&groupkey) {
				None => {}
				Some(v) => {
					match v.get(&i) {
						Some(f) => { row[i] = f.clone() },
						None => {},
					}
				}
			}
		}
		Ok(row)
	}
	
	fn memorize(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> CliResult<()>	 {
		let groupkey = self.groupbykey(groupkey, record, self.groupby.as_ref())?;
		for &i in self.select.iter().filter(|&j| &record[*j] != b"") {
            match self.grouper.entry(groupkey.clone()) {
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
		Ok(())
	}
}