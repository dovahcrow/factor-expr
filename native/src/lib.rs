mod ops;
pub(crate) mod python;
mod replay;
mod ticker_batch;

pub use self::python::*;
use pyo3::{prelude::*, wrap_pyfunction};

#[pymodule]
fn _lib(_: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Factor>()?;
    m.add_function(wrap_pyfunction!(replay, m)?)?;

    Ok(())
}
