//! IO for the PostgreSQL database connected to Substrate Archive Node
pub mod models;
pub mod schema;
use tokio::runtime::Runtime;
use crate::error::Error as ArchiveError;

/// Database object containing a postgreSQL connection and a runtime for asynchronous updating
pub struct Database {
    runtime: Runtime
}

impl Database {
    /// Creates the tables needed for the archive node
    fn spawn_tables() -> Result<(), ArchiveError> {
        Ok(())
    }
}
