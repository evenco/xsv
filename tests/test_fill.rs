use CsvRecord;
use workdir::Workdir;

fn compare_column(got: &[CsvRecord], expected: &[String], column: usize, skip_header: bool) {


    for (value, value_expected) in got.iter().skip(if skip_header {1} else {0}).map(|row| &row[column]).zip(expected.iter()) {
		assert_eq!(value, value_expected)
	}
}

#[test]
fn fill_forward_one() {
    let rows = vec![
        svec!["h1", "h2", "h3"],
        svec!["a", "b", "c"],
        svec!["", "d", "e"],
        svec!["f", "g", "h"],
        svec!["", "i", "j"],
    ];

    let wrk = Workdir::new("fill_forward_one").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fill");
    cmd.arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["a", "a", "f", "f"];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_forward_groupby() {
	
    let rows = vec![
        svec!["h1", "h2", "h3"],
        svec!["a", "b", "c"],
        svec!["", "b", "e"],
        svec!["f", "c", "h"],
        svec!["", "c", "j"],
        svec!["", "b", "j"],
        svec!["", "c", "j"],
    ];
	
    let wrk = Workdir::new("fill_forward_groupby").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fill");
    cmd.args(&vec!["-g","2"]).arg("--").arg("1").arg("in.csv");
	
	let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
	let expected = svec!["a", "a", "f", "f", "a", "f"];
    compare_column(&got, &expected, 0, true);
	
}