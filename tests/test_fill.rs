use quickcheck::TestResult;

use {CsvRecord, qcheck};
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

    let wrk = Workdir::new("fixlengths_all_maxlen_trims").flexible(true);
    wrk.create("in.csv", rows);

    let mut cmd = wrk.command("fill");
    cmd.arg("--").arg("1").arg("in.csv");

    let got: Vec<CsvRecord> = wrk.read_stdout(&mut cmd);
    assert_eq!(got[1][0], "a");
    assert_eq!(got[2][0], "a");
    assert_eq!(got[3][0], "f");
    assert_eq!(got[4][0], "f");
}