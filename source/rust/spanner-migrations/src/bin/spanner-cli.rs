use std::path::Path;

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

const KNOWN_PATH: &str = "/opt/homebrew/share/google-cloud-sdk/bin/spanner-cli";

#[cfg(unix)]
fn exit_code(exit: std::process::ExitStatus) -> i32 {
    exit.code()
        .unwrap_or_else(|| exit.signal().map_or(1, |signal| 128 + signal))
}

#[cfg(not(unix))]
fn exit_code(exit: std::process::ExitStatus) -> i32 {
    exit.code().unwrap_or(1)
}

fn main() {
    if Path::new(KNOWN_PATH).exists() {
        let mut pid = std::process::Command::new(KNOWN_PATH)
            .args(std::env::args_os().skip(1))
            .spawn()
            .unwrap_or_else(|err| {
                eprintln!("failed to spawn spanner-cli: {}", err);
                std::process::exit(1);
            });
        let exit = pid.wait().unwrap_or_else(|err| {
            eprintln!("failed while waiting for spanner-cli: {}", err);
            std::process::exit(1);
        });
        std::process::exit(exit_code(exit));
    }
    eprintln!(
        "spanner-cli not found at its known location.
if you haven't run this tool before, the following should help; it's on you to have gcloud:

```console
gcloud components install spanner-cli
```
"
    );
    std::process::exit(127);
}
