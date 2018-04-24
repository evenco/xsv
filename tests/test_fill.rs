use CsvRecord;
use workdir::Workdir;

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
    assert_eq!(got[1][0], "a");
    assert_eq!(got[2][0], "a");
    assert_eq!(got[3][0], "f");
    assert_eq!(got[4][0], "f");
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
	for (v, v_expected) in got.iter().skip(1).map(|row| &row[0]).zip(expected.iter()) {
		assert_eq!(v, v_expected)
	}
	
}