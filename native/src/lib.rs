mod float;
mod ops;
pub(crate) mod python;
pub mod replay;
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
    m.add(
        "__build__",
        pyo3_built!(py, build, "build", "time", "features", "host", "target"),
    )?;
    m.add_class::<Factor>()?;
    m.add_function(wrap_pyfunction!(python::replay, m)?)?;
    m.add_function(wrap_pyfunction!(python::replay_file, m)?)?;

    Ok(())
}
