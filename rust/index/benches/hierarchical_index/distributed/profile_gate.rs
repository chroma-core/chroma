//! The profiler acknowledges phase boundaries before timed work starts.
use serde_json::{json, Value};
use std::{path::PathBuf, time::Instant};
fn gate_check(ok: bool, message: impl Into<String>) -> Result<(), Box<dyn std::error::Error>> {
    if ok {
        Ok(())
    } else {
        Err(message.into().into())
    }
}
// The external collector acknowledges enabled/disabled profiling before the
// benchmark enters/leaves the timed phase. Waiting is outside balance timing.
pub struct ProfileGate {
    directory: PathBuf,
    phase: Value,
}
pub fn phase_timestamps() -> Value {
    #[repr(C)]
    struct Timespec {
        seconds: std::os::raw::c_long,
        nanos: std::os::raw::c_long,
    }
    unsafe extern "C" {
        fn clock_gettime(clock: std::os::raw::c_int, time: *mut Timespec) -> std::os::raw::c_int;
    }
    #[cfg(target_os = "macos")]
    let clock = 6;
    #[cfg(not(target_os = "macos"))]
    let clock = 1;
    let mut time = Timespec {
        seconds: 0,
        nanos: 0,
    };
    let monotonic = if unsafe { clock_gettime(clock, &mut time) } == 0 {
        Some(time.seconds as u64 * 1_000_000_000 + time.nanos as u64)
    } else {
        None
    };
    json!({"realtime_ns":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos().min(u64::MAX as u128) as u64,"monotonic_ns":monotonic})
}
impl ProfileGate {
    fn publish(&self, name: &str, value: &Value) -> Result<(), Box<dyn std::error::Error>> {
        let temporary = self.directory.join(format!("{name}.tmp"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        serde_json::to_writer(&mut file, value)?;
        file.sync_all()?;
        std::fs::hard_link(&temporary, self.directory.join(name))?;
        std::fs::remove_file(temporary)?;
        Ok(())
    }
    fn wait(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + std::time::Duration::from_secs(60);
        loop {
            if let Ok(file) = std::fs::File::open(self.directory.join(name)) {
                let reply: Value = serde_json::from_reader(file)?;
                gate_check(
                    ["format_version", "phase_id", "pid"]
                        .iter()
                        .all(|key| reply[key] == self.phase[key]),
                    "profile gate acknowledgement binding differs",
                )?;
                return Ok(());
            }
            gate_check(
                Instant::now() < deadline,
                format!("profile gate timeout waiting for {name}"),
            )?;
            if let Ok(value) = std::env::var("BENCH_ABSOLUTE_DEADLINE_UNIX") {
                let deadline: f64 = value.parse()?;
                gate_check(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs_f64()
                        < deadline,
                    "absolute workload deadline while waiting for profile gate",
                )?;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    pub fn before(rows: u64, start: u64) -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Some(directory) = std::env::var_os("BENCH_PROFILE_GATE") else {
            return Ok(None);
        };
        gate_check(
            rows == 100_000 && start == 0,
            "profile gate supports one 100K balance only",
        )?;
        let mut phase = phase_timestamps();
        phase["format_version"] = json!(1);
        phase["phase_id"] = json!("balance-0");
        phase["rows"] = json!(rows);
        phase["pid"] = json!(std::process::id());
        let gate = Self {
            directory: directory.into(),
            phase,
        };
        gate.publish("ready.json", &gate.phase)?;
        gate.wait("go.json")?;
        Ok(Some(gate))
    }
    pub fn after(&self, elapsed: f64, success: bool) -> Result<(), Box<dyn std::error::Error>> {
        let mut done = self.phase.clone();
        let timestamps = phase_timestamps();
        done["realtime_ns"] = timestamps["realtime_ns"].clone();
        done["monotonic_ns"] = timestamps["monotonic_ns"].clone();
        done["elapsed_seconds"] = json!(elapsed);
        done["status"] = json!(if success { "completed" } else { "failed" });
        self.publish("done.json", &done)?;
        self.wait("released.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> ProfileGate {
        let dir = std::env::temp_dir().join(format!(
            "spann-profile-gate-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        ProfileGate {
            directory: dir,
            phase: json!({"format_version":1,"phase_id":"balance-0","pid":std::process::id()}),
        }
    }
    #[test]
    fn acknowledgement_is_bound_and_publication_never_overwrites() {
        let gate = fixture();
        gate.publish(
            "go.json",
            &json!({"format_version":1,"phase_id":"wrong","pid":std::process::id()}),
        )
        .unwrap();
        assert!(gate
            .wait("go.json")
            .unwrap_err()
            .to_string()
            .contains("binding"));
        assert!(gate.publish("go.json", &gate.phase).is_err());
        std::fs::remove_dir_all(gate.directory).unwrap();
    }
    #[test]
    fn completion_records_status_and_waits_for_matching_release() {
        let gate = fixture();
        gate.publish("released.json", &gate.phase).unwrap();
        gate.after(0.25, false).unwrap();
        let done: Value =
            serde_json::from_slice(&std::fs::read(gate.directory.join("done.json")).unwrap())
                .unwrap();
        assert_eq!(done["status"], "failed");
        assert_eq!(done["elapsed_seconds"], 0.25);
        assert!(done["realtime_ns"].as_u64().unwrap() > 0);
        assert!(done["monotonic_ns"].as_u64().unwrap() > 0);
        std::fs::remove_dir_all(gate.directory).unwrap();
    }
}
