use std::path::Path;

const KNOWN_PATH: &str = "/opt/homebrew/share/google-cloud-sdk/bin/spanner-cli";

fn main() {
    if Path::new(KNOWN_PATH).exists() {
        let mut pid = std::process::Command::new(KNOWN_PATH)
            .args(std::env::args_os().skip(1))
            .spawn()
            .expect("failed to spawn spanner-cli");
        let exit = pid
            .wait()
            .expect("could not wait for spanner-cli; zombies?");
        std::process::exit(exit.code().unwrap_or(if exit.success() { 0 } else { 1 }));
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
