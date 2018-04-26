use std::collections::hash_map::HashMap;
use std::io;
use std::iter;
use std::ops;

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

The option `--first` fills empty values using the first 
seen non-empty value in that column, instead of the most 
recent non-empty value in that column.

The option `--backfill` fills empty values at the start of
the CSV with the first valid value in that column. This
requires buffering rows with empty values in the target
column which appear before the first valid value.

The option `--groupby` groups the rows by the specified
columns before filling in the empty values. Using this
option, empty values are only filled with values which
belong to the same group of rows, as determined by the
columns selected in the `--groupby` option.

When both `--groupby` and `--backfill` are specified, and the
CSV is not sorted by the `--groupby` columns, rows may be
re-ordered during output due to the buffering of rows
collected before the first valid value.

Usage:
    xsv fill [options] [--] <selection> [<input>]
    xsv fill --help

fill options:
    -g --groupby <keys>    Group by specified columns.
    -f --first             Fill using the first valid value of a column, instead of the latest.
    -p --backfill          Fill initial empty values with the first valid value.

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
    flag_backfill: bool
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

    let filler = Filler::new(groupby, select)
        .use_first_value(args.flag_first)
        .backfill_empty_values(args.flag_backfill);
    filler.fill(&mut rdr, &mut wtr)
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
struct VecRecord(Vec<ByteString>);

impl VecRecord {
    fn from_record(record: &csv::ByteRecord) -> Self {
        VecRecord(record.iter().map(|f| f.to_vec()).collect())
    }
}

impl iter::FromIterator<ByteString> for VecRecord {
    fn from_iter<T: IntoIterator<Item = ByteString>>(iter: T) -> Self {
        VecRecord(Vec::from_iter(iter))
    }
}

impl iter::IntoIterator for VecRecord {
    type Item = ByteString;
    type IntoIter = ::std::vec::IntoIter<ByteString>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ops::Deref for VecRecord {
    type Target = [ByteString];
    fn deref(&self) -> &[ByteString] {
        &self.0
    }
}


type GroupBuffer = HashMap<Option<VecRecord>, Vec<VecRecord>>;
type GroupValues = HashMap<usize, ByteString>;
type Grouper = HashMap<Option<VecRecord>, GroupValues>;
type GroupKey = Option<Selection>;

trait _GroupKey {
    fn key(&self, record: &csv::ByteRecord) -> Result<Option<VecRecord>, String>;
}

impl _GroupKey for GroupKey {
    fn key(&self, record: &csv::ByteRecord) -> Result<Option<VecRecord>, String> {
         match *self {
                Some(ref value) => Ok(Some(value.iter().map(|&i| record[i].to_vec()).collect())),
                None => Ok(None),
        }
    }
}

trait GroupMemorizer {
    fn fill(&self, selection: &Selection, record: VecRecord) ->  VecRecord;
    fn memorize(&mut self, selection: &Selection, record: &csv::ByteRecord);
    fn memorize_first(&mut self, selection: &Selection, record: &csv::ByteRecord);
}

impl GroupMemorizer for GroupValues {
    fn memorize(&mut self, selection: &Selection, record: &csv::ByteRecord) {
        for &col in selection.iter().filter(|&col| !record[*col].is_empty()) {
            self.insert(col, record[col].to_vec());
        }
    }

    fn memorize_first(&mut self, selection: &Selection, record: &csv::ByteRecord) {
        for &col in selection.iter().filter(|&col| !record[*col].is_empty()) {
            self.entry(col).or_insert(record[col].to_vec());
        }
    }

    fn fill(&self, selection: &Selection, record: VecRecord) -> VecRecord {
        record.into_iter().enumerate().map_selected(selection, |(col, field)|{
            (col, if field.is_empty() { self.get(&col).unwrap_or(&field).to_vec() } else { field })
        }).map(|(_, field)| field).collect()
    }
}

struct Filler {
    grouper: Grouper,
    groupby: GroupKey,
    select: Selection,
    buffer: GroupBuffer,
    first: bool,
    backfill: bool
}

impl Filler {
    fn new(groupby: GroupKey, select: Selection) -> Self {
        Self {
            grouper: Grouper::new(),
            groupby: groupby,
            select: select,
            buffer: GroupBuffer::new(),
            first: false,
            backfill: false,
        }
    }

    fn use_first_value(mut self, first: bool) -> Self {
        self.first = first;
        self
    }

    fn backfill_empty_values(mut self, backfill: bool) -> Self {
        self.backfill = backfill;
        self
    }
    
    fn fill(mut self, rdr: &mut BoxedReader, wtr: &mut BoxedWriter) -> CliResult<()> {
        let mut record = csv::ByteRecord::new();

        while rdr.read_byte_record(&mut record)? {
            // Precompute groupby key
            let key = self.groupby.key(&record)?;

            // Record valid fields, and fill empty fields
            let group = self.grouper.entry(key.clone()).or_insert_with(HashMap::new);

            if self.first {
                group.memorize_first(&self.select, &record);
            } else {
                group.memorize(&self.select, &record);
            }
            
            let row = group.fill(&self.select, VecRecord::from_record(&record));

            // Handle buffering rows which still have nulls.
            if self.backfill && (self.select.iter().any(|&i| row[i] == b"")) {
                self.buffer.entry(key.clone()).or_insert_with(Vec::new).push(row);
            } else {
                if let Some(rows) = self.buffer.remove(&key) {
                    for buffered_row in rows {
                        wtr.write_record(group.fill(&self.select, buffered_row).iter())?;
                    }
                }
                wtr.write_record(row.iter())?;
            }
        }

        // Ensure any remaining buffers are dumped at the end.
        for (key, rows) in self.buffer {
            let group = self.grouper.get(&key).unwrap();
            for buffered_row in rows {
                wtr.write_record(group.fill(&self.select, buffered_row).iter())?;
            }
        }

        wtr.flush()?;
        Ok(())
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