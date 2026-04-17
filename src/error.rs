//! Error types for `oxn`.
//!
//! [`enum@Error`] is the crate's single error enum. The negative integer
//! codes returned by Lua scripts (mirroring BullMQ's scheme) are translated
//! into typed [`ScriptError`] variants so callers do not have to match on
//! magic numbers.

use std::io;

use thiserror::Error;

/// Crate-wide `Result` alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by `oxn`.
#[derive(Debug, Error)]
pub enum Error {
    /// The queue or job was not found in the backend.
    #[error("not found: {0}")]
    NotFound(String),

    /// Serialization / deserialization failed for the job payload or metadata.
    #[error("serialization failed: {0}")]
    Serde(#[from] serde_json::Error),

    /// Typed Lua script error (see [`ScriptError`]).
    #[error(transparent)]
    Script(#[from] ScriptError),

    /// The Redis client returned an error.
    #[cfg(feature = "redis-backend")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-backend")))]
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),

    /// Connection-pool error.
    #[cfg(feature = "redis-backend")]
    #[cfg_attr(docsrs, doc(cfg(feature = "redis-backend")))]
    #[error("redis pool: {0}")]
    Pool(String),

    /// Job processing should be retried after the given delay.
    ///
    /// Return this from a job handler to instruct the worker to postpone the
    /// job. Corresponds to BullMQ's `DelayedError` — but plumbed through the
    /// return type instead of a thrown exception.
    #[error("job delayed by {delay_ms}ms")]
    Delayed {
        /// Delay in milliseconds before the job is eligible again.
        delay_ms: u64,
    },

    /// The queue's rate limit was hit; the worker backs off and retries.
    #[error("rate limited for {delay_ms}ms")]
    RateLimited {
        /// Delay in milliseconds before the next fetch is attempted.
        delay_ms: u64,
    },

    /// Job failed in a way that should not trigger retries.
    #[error("unrecoverable: {0}")]
    Unrecoverable(String),

    /// The parent job is blocked waiting on child dependencies to finish.
    #[cfg(feature = "flow")]
    #[cfg_attr(docsrs, doc(cfg(feature = "flow")))]
    #[error("waiting on child dependencies")]
    WaitingChildren,

    /// A configuration value is invalid.
    #[error("invalid config: {0}")]
    Config(String),

    /// The handler returned an error.
    #[error("handler error: {0}")]
    Handler(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// I/O error from the runtime or network.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The backend was asked to perform an operation after shutdown.
    #[error("backend closed")]
    Closed,
}

impl Error {
    /// Wrap an arbitrary handler error.
    pub fn handler<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Handler(Box::new(err))
    }

    /// Convenience constructor for [`Error::Unrecoverable`].
    pub fn unrecoverable<S: Into<String>>(msg: S) -> Self {
        Self::Unrecoverable(msg.into())
    }

    /// `true` if re-queueing the job is pointless.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Unrecoverable(_) | Self::NotFound(_))
    }
}

/// Typed variants of the negative integer codes returned from Lua scripts.
///
/// BullMQ uses raw negative ints; `oxn` translates them at the boundary so
/// the rest of the codebase never has to match on `-4`.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScriptError {
    /// The referenced job's hash does not exist.
    #[error("job does not exist")]
    JobNotFound,
    /// The job lock key is missing (expired or already released).
    #[error("lock does not exist")]
    LockNotFound,
    /// The Lua script found the job in a state it wasn't expecting
    /// (e.g. retrying a job that's already completed).
    #[error("job is not in the expected state")]
    NotInExpectedState,
    /// A parent job still has pending children and cannot be resolved.
    #[error("parent still has pending children")]
    PendingChildren,
    /// A child references a parent whose hash isn't present.
    #[error("parent job does not exist")]
    ParentNotFound,
    /// The token passed to a finalization script doesn't match the held lock.
    #[error("lock token does not match worker token")]
    LockMismatch,
    /// A parent has children that failed permanently.
    #[error("parent has failed children")]
    FailedChildren,
    /// Reserved for future or unmapped script return codes.
    #[error("unknown script error code ({0})")]
    Unknown(
        /// The raw integer returned by the Lua script.
        i64,
    ),
}

impl ScriptError {
    /// Translate a Lua return code into a typed variant.
    pub fn from_code(code: i64) -> Self {
        match code {
            -1 => Self::JobNotFound,
            -2 => Self::LockNotFound,
            -3 => Self::NotInExpectedState,
            -4 => Self::PendingChildren,
            -5 => Self::ParentNotFound,
            -6 => Self::LockMismatch,
            -9 => Self::FailedChildren,
            other => Self::Unknown(other),
        }
    }
}

#[cfg(feature = "redis-backend")]
impl From<deadpool_redis::PoolError> for Error {
    fn from(e: deadpool_redis::PoolError) -> Self {
        Self::Pool(e.to_string())
    }
}

#[cfg(feature = "redis-backend")]
impl From<deadpool_redis::CreatePoolError> for Error {
    fn from(e: deadpool_redis::CreatePoolError) -> Self {
        Self::Pool(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_error_codes_cover_bullmq_table() {
        assert_eq!(ScriptError::from_code(-1), ScriptError::JobNotFound);
        assert_eq!(ScriptError::from_code(-2), ScriptError::LockNotFound);
        assert_eq!(
            ScriptError::from_code(-3),
            ScriptError::NotInExpectedState
        );
        assert_eq!(ScriptError::from_code(-4), ScriptError::PendingChildren);
        assert_eq!(ScriptError::from_code(-5), ScriptError::ParentNotFound);
        assert_eq!(ScriptError::from_code(-6), ScriptError::LockMismatch);
        assert_eq!(ScriptError::from_code(-9), ScriptError::FailedChildren);
        assert_eq!(ScriptError::from_code(-42), ScriptError::Unknown(-42));
    }

    #[test]
    fn terminal_errors() {
        assert!(Error::Unrecoverable("x".into()).is_terminal());
        assert!(Error::NotFound("y".into()).is_terminal());
        assert!(!Error::Delayed { delay_ms: 100 }.is_terminal());
        assert!(!Error::RateLimited { delay_ms: 100 }.is_terminal());
        assert!(!Error::Closed.is_terminal());
    }

    #[test]
    fn handler_wraps_arbitrary_error() {
        #[derive(Debug, thiserror::Error)]
        #[error("bar")]
        struct Inner;
        let e = Error::handler(Inner);
        assert!(matches!(e, Error::Handler(_)));
    }

    #[test]
    fn display_delayed_formats_delay() {
        let s = Error::Delayed { delay_ms: 750 }.to_string();
        assert!(s.contains("750"));
    }
}

