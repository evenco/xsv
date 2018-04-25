use std::collections::hash_map::{Entry, HashMap};
use std::io;
use std::iter;

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
    -1 --first             Fill using the first value of a column.

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

type BoxedWriter = csv::Writer<Box<io::Write + 'static>>;
type BoxedReader = csv::Reader<Box<io::Read + 'static>>;

#[derive(Deserialize)]
struct Args {
    arg_input: Option<String>,
    arg_selection: SelectColumns,
    flag_output: Option<String>,
    flag_no_headers: bool,
    flag_delimiter: Option<Delimiter>,
    flag_groupby: Option<SelectColumns>,
    flag_first: bool,
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

    if args.flag_first {
        FirstFill::new(groupby, select).fill(&mut rdr, &mut wtr)
    } else {
        ForwardFill::new(groupby, select).fill(&mut rdr, &mut wtr)
    }
}

type VecRecord = Vec<ByteString>;
type GroupValues = HashMap<usize, ByteString>;
type Grouper = HashMap<VecRecord, GroupValues>;

trait Filler {
    fn groupbykey(
        &self,
        groupkey: Option<VecRecord>,
        record: &csv::ByteRecord,
        groupby: Option<&Selection>,
    ) -> Result<VecRecord, String> {
        match groupkey {
            Some(value) => Ok(value),
            None => Ok(match groupby {
                Some(ref value) => value.iter().map(|&i| record[i].to_vec()).collect(),
                None => vec![],
            }),
        }
    }

    fn fill(self, rdr: &mut BoxedReader, wtr: &mut BoxedWriter) -> CliResult<()>;
    fn memorize(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> CliResult<()>;
    fn filledvalues(
        &mut self,
        record: &csv::ByteRecord,
        groupkey: Option<VecRecord>,
    ) -> Result<VecRecord,String>;
}

trait GroupFiller {
    fn fill(&self, groupkey: &VecRecord, col: usize, field: ByteString) -> ByteString;
    fn memorize(&mut self, groupkey: &VecRecord, col: usize, field: ByteString);
    fn memorize_first(&mut self, groupkey: &VecRecord, col: usize, field: ByteString);
}

impl GroupFiller for Grouper {
    
    fn fill(&self, groupkey: &VecRecord, col: usize, field: ByteString) -> ByteString {
        if field != b"" {
            return field;
        }
        match self.get(groupkey) {
            None => field,
            Some(v) => match v.get(&col) {
                Some(f) => f.clone(),
                None => field,
            },
        }
    }

    fn memorize(&mut self, groupkey: &VecRecord, col: usize, field: ByteString)
    {
        match self.entry(groupkey.clone()) {
                Entry::Vacant(v) => {
                    let mut values = HashMap::new();
                    values.insert(col, field);
                    v.insert(values);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().insert(col, field);
                }
            }
    }

    fn memorize_first(&mut self, groupkey: &VecRecord, col: usize, field: ByteString) {
                match self.entry(groupkey.clone()) {
                Entry::Vacant(v) => {
                    let mut values = HashMap::new();
                    values.entry(col).or_insert(field);
                    v.insert(values);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().entry(col).or_insert(field);
                }
            }
    }
}

trait Fillable {
    fn fill(&self, selection: &Selection, grouper: &Grouper, groupbykey: &VecRecord) -> VecRecord;
}

impl Fillable for csv::ByteRecord {
    fn fill(&self, selection: &Selection, grouper: &Grouper, groupbykey: &VecRecord) -> VecRecord {
        self.iter().map(|f| f.to_vec()).enumerate().map_selected(selection, 
             |(i, field)| (i, grouper.fill(groupbykey, i, field))).map(
             |(_, field)| field).collect()
    }
}

impl Fillable for VecRecord {
    fn fill(&self, selection: &Selection, grouper: &Grouper, groupbykey: &VecRecord) -> VecRecord {
        self.iter().map(|f| f.to_vec()).enumerate().map_selected(selection, 
             |(i, field)| (i, grouper.fill(groupbykey, i, field))).map(
             |(_, field)| field).collect()
    }
}

struct ForwardFill {
    grouper: Grouper,
    groupby: Option<Selection>,
    select: Selection,
}

impl ForwardFill {
    fn new(groupby: Option<Selection>, select: Selection) -> Self {
        ForwardFill {
            grouper: Grouper::new(),
            groupby: groupby,
            select: select,
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

    fn filledvalues(
        &mut self,
        record: &csv::ByteRecord,
        groupkey: Option<VecRecord>,
    ) -> Result<VecRecord,String> {
        let groupkey = self.groupbykey(groupkey, record, self.groupby.as_ref())?;
        let row = record.fill(&self.select, &self.grouper, &groupkey);
        Ok(row)
    }
    


    fn memorize(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> CliResult<()> {
        let groupkey = self.groupbykey(groupkey, record, self.groupby.as_ref())?;
        for (i, field) in self.select
            .iter()
            .map(|&i| (i, &record[i]))
            .filter(|&(_i, field)| field != b"")
        {
            self.grouper.memorize(&groupkey, i, field.to_vec())
        }

        Ok(())
    }
}

struct FirstFill {
	grouper: Grouper,
    groupby: Option<Selection>,
    select: Selection,
    buffer: HashMap<VecRecord, Vec<VecRecord>>,
}

impl FirstFill {
	
	fn new(groupby: Option<Selection>, select: Selection) -> Self {
		Self {
			grouper: Grouper::new(),
			groupby: groupby,
			select: select,
			buffer: HashMap::new(),
		}
	}
}

impl Filler for FirstFill {

    
    fn fill(mut self, rdr: &mut BoxedReader, wtr: &mut BoxedWriter) -> CliResult<()>
    {
        let mut record = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut record)? {
            // Precompute groupby key
            let groupbykey = Some(self.groupbykey(None, &record, self.groupby.as_ref())?);
            let groupkey = groupbykey.clone().unwrap();
            self.memorize(&record, groupbykey.clone())?;
            let row = self.filledvalues(&record, groupbykey.clone())?;
            
            if self.select.iter().any(|&i| row[i] == b"") {
                self.buffer.entry(groupbykey.unwrap()).or_insert_with(|| Vec::new()).push(row);
            } else {
                if let Some(rows) = self.buffer.remove(&groupkey) {
                    for buffered_row in rows {
                        wtr.write_record(buffered_row.fill(&self.select, &self.grouper, &groupkey).iter())?;
                    }
                }
                wtr.write_record(row.iter())?;
            }
        }
        for (key, rows) in self.buffer {
            for buffered_row in rows {
                wtr.write_record(buffered_row.fill(&self.select, &self.grouper, &key).iter())?;
            }
        }

        wtr.flush()?;
        Ok(())
    }
    
    fn memorize(&mut self, record: &csv::ByteRecord, groupkey: Option<VecRecord>) -> CliResult<()>
    {
        let groupkey = self.groupbykey(groupkey, record, self.groupby.as_ref())?;
        for (i, field) in self.select
            .iter()
            .map(|&i| (i, &record[i]))
            .filter(|&(_i, field)| field != b"")
        {
            self.grouper.memorize_first(&groupkey, i, field.to_vec())
        }

        Ok(())
    }
    
    fn filledvalues(
        &mut self,
        record: &csv::ByteRecord,
        groupkey: Option<VecRecord>,
    ) -> Result<VecRecord, String>
    {
        let groupkey = self.groupbykey(groupkey, record, self.groupby.as_ref())?;
        let row = record.fill(&self.select, &self.grouper, &groupkey);
        Ok(row)
    }

}

struct MapSelected<I, F> {
    selection: Vec<usize>,
    selection_index: usize,
    index: usize,
    iterator: I,
    predicate: F,
}

impl<I: iter::Iterator, F> iter::Iterator for MapSelected<I, F>
where
    F: FnMut(I::Item) -> I::Item,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iterator.next()?;
        let result = match self.selection_index {
            ref mut sidx if (self.selection.get(*sidx) == Some(&self.index)) => {
                *sidx += 1;
                Some((self.predicate)(item))
            }
            _ => Some(item),
        };
        self.index += 1;
        result
    }
}

trait Selectable<B>
where Self : iter::Iterator<Item=B> + Sized
{
    fn map_selected<F>(self, selector: &Selection, predicate: F) -> MapSelected<Self, F>
    where F: FnMut(B) -> B;
}

impl<B, C> Selectable<B> for C
where C : iter::Iterator<Item=B> + Sized
{
    fn map_selected<F>(self, selector: &Selection, predicate: F) -> MapSelected<Self, F> 
    where F: FnMut(B) -> B,
    {
        MapSelected {
            selection: selector.iter().map(|&x| x).collect(),
            selection_index: 0,
            index: 0,
            iterator: self,
            predicate: predicate,
        }
    }
}