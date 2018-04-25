use CsvRecord;
use workdir::Workdir;

fn compare_column(got: &[CsvRecord], expected: &[String], column: usize, skip_header: bool) {
    for (value, value_expected) in got.iter()
        .skip(if skip_header { 1 } else { 0 })
        .map(|row| &row[column])
        .zip(expected.iter())
    {
        assert_eq!(value, value_expected)
    }
}

fn simple_rows() -> Vec<Vec<String>> {
    vec![
        svec!["h1", "h2", "h3"],
        svec!["", "b", "c"],
        svec!["a", "b", "c"],
        svec!["", "d", ""],
        svec!["f", "g", ""],
        svec!["", "i", "j"],
    ]
}

#[test]
fn coalesce() {
    let rows = simple_rows();
    
    let wrk = Workdir::new("coalesce").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("coalesce");
    cmd.arg("--").arg("1,3").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["c", "a", "", "f", "j"];
    compare_column(&got, &expected, 3, true);
}

#[test]
fn coalesce_with_name() {
    let rows = simple_rows();

    let wrk = Workdir::new("coalesce_with_name").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("coalesce");
    cmd.args(vec!["--name", "h4"]).arg("--").arg("1,3").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["c", "a", "", "f", "j"];
    compare_column(&got, &expected, 3, true);

    assert_eq!(got[0][3], "h4");
}