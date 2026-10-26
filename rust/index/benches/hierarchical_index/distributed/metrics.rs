//! These cumulative process measurements include protocol and worker CPU.
//! Differences across drained phase boundaries support cost accounting; they
//! are not a direct measurement of how many construction workers are busy.
pub fn cpu_seconds() -> Option<f64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields: Vec<_> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let user: f64 = fields.get(11)?.parse().ok()?;
    let system: f64 = fields.get(12)?.parse().ok()?;
    static HZ: std::sync::OnceLock<Option<f64>> = std::sync::OnceLock::new();
    let hz = HZ.get_or_init(|| {
        let output = std::process::Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let value: f64 = String::from_utf8(output.stdout).ok()?.trim().parse().ok()?;
        (value > 0.).then_some(value)
    });
    Some((user + system) / (*hz)?)
}
