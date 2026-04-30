/// Execute the `foundation version` command.
///
/// Prints the version, commit SHA, and build date. Returns an exit code.
pub fn execute() -> i32 {
    let version = env!("FOUNDATION_VERSION");
    let commit = env!("FOUNDATION_COMMIT");
    let date = env!("FOUNDATION_DATE");

    println!("foundation {version}");
    println!("commit:    {commit}");
    println!("date:      {date}");

    0
}
