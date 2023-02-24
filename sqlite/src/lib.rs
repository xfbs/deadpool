#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(
    nonstandard_style,
    rust_2018_idioms,
    rustdoc::broken_intra_doc_links,
    rustdoc::private_intra_doc_links
)]
#![forbid(non_ascii_idents, unsafe_code)]
#![warn(
    deprecated_in_future,
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    unused_import_braces,
    unused_labels,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]
#![allow(clippy::uninlined_format_args)]

mod config;

use rusqlite::{Connection as SqlConnection, Error as SqlError};
use std::fmt::{Debug, Error as FmtError, Formatter};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use deadpool::{
    async_trait,
    managed::{self, RecycleError},
};
use deadpool_sync::SyncWrapper;

pub use deadpool::managed::reexports::*;
pub use deadpool_sync::reexports::*;
pub use rusqlite;

deadpool::managed_reexports!(
    "rusqlite",
    Manager,
    deadpool::managed::Object<Manager>,
    rusqlite::Error,
    ConfigError
);

#[derive(Clone)]
struct ConnectFunction(Arc<dyn Fn(PathBuf) -> Result<SqlConnection, SqlError> + Send + Sync>);

impl Default for ConnectFunction {
    fn default() -> Self {
        ConnectFunction(Arc::new(|path| SqlConnection::open(path)))
    }
}

impl Debug for ConnectFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        f.debug_struct("ConnectFunction").finish()
    }
}

pub use self::config::{Config, ConfigError};

/// Type alias for [`Object`]
pub type Connection = Object;

/// [`Manager`] for creating and recycling SQLite [`Connection`]s.
///
/// [`Manager`]: managed::Manager
#[derive(Debug)]
pub struct Manager {
    config: Config,
    recycle_count: AtomicUsize,
    runtime: Runtime,
    connect: ConnectFunction,
}

impl Manager {
    /// Creates a new [`Manager`] using the given [`Config`] backed by the
    /// specified [`Runtime`].
    #[must_use]
    pub fn from_config(config: &Config, runtime: Runtime) -> Self {
        Self {
            config: config.clone(),
            recycle_count: AtomicUsize::new(0),
            runtime,
            connect: ConnectFunction::default(),
        }
    }

    /// Overwrite the connection create function.
    ///
    /// By default, this function calls [Connection::open](rusqlite::Connection::open) to establish
    /// a database connection. By overwriting it, you can add custom code to be run after
    /// connecting, such as overriding built-in functions or enabling flags when opening the
    /// database.
    ///
    /// ```rust
    /// # use rusqlite::{Connection, OpenFlags};
    /// # use deadpool_sqlite::Manager;
    /// # use deadpool::Runtime;
    /// let mut manager = Manager::from_config(&Default::default(), Runtime::Tokio1);
    /// manager.set_connect_function(|path| {
    ///     let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    ///     Ok(conn)
    /// });
    /// ```
    pub fn set_connect_function<
        F: Fn(PathBuf) -> Result<SqlConnection, SqlError> + Send + Sync + 'static,
    >(
        &mut self,
        func: F,
    ) {
        self.connect = ConnectFunction(Arc::new(func));
    }
}

#[async_trait]
impl managed::Manager for Manager {
    type Type = SyncWrapper<SqlConnection>;
    type Error = SqlError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let path = self.config.path.clone();
        let connect = self.connect.clone();
        SyncWrapper::new(self.runtime, move || connect.0(path)).await
    }

    async fn recycle(&self, conn: &mut Self::Type) -> managed::RecycleResult<Self::Error> {
        if conn.is_mutex_poisoned() {
            return Err(RecycleError::Message(
                "Mutex is poisoned. Connection is considered unusable.".into(),
            ));
        }
        let recycle_count = self.recycle_count.fetch_add(1, Ordering::Relaxed);
        let n: usize = conn
            .interact(move |conn| conn.query_row("SELECT $1", [recycle_count], |row| row.get(0)))
            .await
            .map_err(|e| RecycleError::Message(format!("{}", e)))??;
        if n == recycle_count {
            Ok(())
        } else {
            Err(RecycleError::StaticMessage("Recycle count mismatch"))
        }
    }
}
