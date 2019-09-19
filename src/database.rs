use futures::{Future, Stream};
use tokio::Runtime;
use tokio_postgres::{Client, NoTls, impls::Connect, tls::MakeTlsConnect, Socket };
//! IO for the PostgreSQL database connected to Substrate Archive Node

/// Database object containing a postgreSQL connection and a runtime for asynchronous updating
pub struct Database {
    connection: Client,
    runtime: Runtime
}

/// Represents the different kinds of database connections possible
// TODO: No TLS connection
pub enum Connect {
    Default,
    User(String),
    HostAndUser(String, String)
}

impl Database {

    /// A connects to a PostgreSQL database
    /// Creates the tables if not already there
    pub fn new(connect: Connect, runtime: Runtime, tls: bool) -> Self {
        let connection = match connect {

            Default => {
                Client::connect("host=localhost user=postgres", NoTls)?
            },
            User(u) => {
                C
            },
            HostAndUser(user, host) => {


            }
        };

        Self::spawn_tables();
        unimplemented!();
    }

    /// Creates the tables needed for the archive node
    fn spawn_tables() -> Result<(), ArchiveError> {
        unimplemented!()
    }
}
