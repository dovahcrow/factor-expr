mod float;
mod ops;
pub(crate) mod python;
mod replay;
mod ticker_batch;

pub use self::python::*;
use pyo3::{prelude::*, wrap_pyfunction};
use pyo3_built::pyo3_built;

#[allow(dead_code)]
mod build {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[pymodule]
fn _lib(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("__build__", pyo3_built!(py, build))?;
    m.add_class::<Factor>()?;
    m.add_function(wrap_pyfunction!(replay, m)?)?;

    Ok(())
}
