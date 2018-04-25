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

#[test]
fn fill_forward() {
    let rows = vec![
        svec!["h1", "h2", "h3"],
        svec!["", "b", "c"],
        svec!["a", "b", "c"],
        svec!["", "d", ""],
        svec!["f", "g", "h"],
        svec!["", "i", "j"],
    ];

    let wrk = Workdir::new("fill_forward").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fill");
    cmd.arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["", "a", "a", "f", "f"];
    compare_column(&got, &expected, 0, true);
    let expected = svec!["c", "c", "", "h", "j"];
    compare_column(&got, &expected, 2, true);
}

#[test]
fn fill_forward_groupby() {
    let rows = vec![
        svec!["h1", "h2", "h3"],
        svec!["", "b", "e"],
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
    cmd.args(&vec!["-g", "2"]).arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["", "a", "a", "f", "f", "a", "f"];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_first_groupby() {
    let rows = vec![
        svec!["h1", "h2", "h3"],
        svec!["", "b", "e"],
        svec!["", "c", "j"],
        svec!["a", "b", "c"],
        svec!["", "b", "e"],
        svec!["f", "c", "h"],
        svec!["", "c", "j"],
        svec!["", "b", "j"],
        svec!["", "c", "j"],
    ];

    let wrk = Workdir::new("fill_first_groupby").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fill");
    cmd.args(&vec!["-g", "2"]).arg("--first").arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["a", "a", "a", "f", "f", "f", "a", "f"];
    compare_column(&got, &expected, 0, true);
}

#[test]
fn fill_first() {
    let rows = vec![
        svec!["h1", "h2", "h3"],
        svec!["", "b", "e"],
        svec!["", "c", "j"],
        svec!["a", "b", "c"],
        svec!["", "b", "e"],
        svec!["f", "c", "h"],
        svec!["", "c", "j"],
        svec!["", "b", "j"],
        svec!["", "c", "j"],
    ];

    let wrk = Workdir::new("fill_first").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fill");
    cmd.arg("--first").arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    let expected = svec!["a", "a", "a", "a", "f", "a", "a", "a"];
    compare_column(&got, &expected, 0, true);
}