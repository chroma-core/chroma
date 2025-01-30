use rust_embed::Embed;

#[derive(Embed)]
#[folder = "../../chromadb/migrations/"]
struct RootMigrationsFolder;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_migration_files() {
        for file in RootMigrationsFolder::iter() {
            println!("File: {}", file);
        }
    }
}
