// We want a few things here:
// 1. A way to create a new error with a backtrace
// 2. A way to centralize setting a breakpoint to trap any error in the system fairly soon
//    after it's created (or at least when it's propagated from a library we use back to us)
// 3. Same but for logging / emitting error messages into the tracing/logging system

use std::borrow::Cow;
use backtrace_error::DynBacktraceError;
use tracing::error;

#[cfg(test)]
use test_log::test;

#[derive(Debug)]
#[allow(dead_code)]
pub struct Error(DynBacktraceError);
pub type Result<T> = std::result::Result<T, Error>;

struct SimpleErr(Cow<'static, str>);
impl std::fmt::Debug for SimpleErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::fmt::Display for SimpleErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for SimpleErr {
    fn description(&self) -> &str {
        &self.0
    }
}

impl<E: std::error::Error + Send + Sync + 'static> From<E> for Error {
    fn from(err: E) -> Error {
        Error::new(err)
    }
}

impl Error {
    pub fn new<E: std::error::Error + Send + Sync + 'static>(err: E) -> Error {
        error!(target: "submerge", "{:?}", err);
        let dbe = DynBacktraceError::from(err);
        Error(dbe)
    }
}

pub fn err(msg: impl Into<Cow<'static, str>>) -> Error {
    let err = SimpleErr(msg.into());
    Error::new(err)
}

#[test]
fn test_error() {
    let _err = err("test error");
}
