#[derive(Debug)]
pub struct SqliteLog {
    db: SqliteDb,
}

// TODO: use the enum facade instead of directly exposing
impl SqliteLog {
    pub fn new(db: SqliteDb) -> Self {
        Self { db }
    }

    pub fn push_logs(&self, logs: Vec<Log>) {
        unimplemented!();
    }
}
