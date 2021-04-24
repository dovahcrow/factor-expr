use super::ops::{from_str, Operator};
use arrow::{array::Array, record_batch::RecordBatch};
use dict_derive::IntoPyObject;
use fehler::throw;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{class::basic::CompareOp, PyObjectProtocol, PySequenceProtocol};
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

type ArrowFFIPtr = (usize, usize);

#[derive(IntoPyObject)]
pub struct ReplayResult {
    index: ArrowFFIPtr,
    succeeded: BTreeMap<usize, ArrowFFIPtr>,
    failed: BTreeMap<usize, String>,
}

#[pyclass]
pub struct Factor {
    op: Box<dyn Operator<RecordBatch>>,
}

#[pymethods]
impl Factor {
    #[new]
    pub fn new(repr: &str) -> PyResult<Self> {
        Ok(Self {
            op: from_str(repr).map_err(|e| PyValueError::new_err(format!("{}", e)))?,
        })
    }

    pub fn ready_offset(&self) -> usize {
        self.op.ready_offset()
    }

    pub fn insert<'p>(&self, i: usize, other: PyRef<'p, Factor>) -> PyResult<Factor> {
        if i == 0 {
            return Ok(Factor {
                op: other.op.clone(),
            });
        }

        let mut op = self.op.clone();
        op.insert(i, other.op.clone())
            .ok_or_else(|| PyValueError::new_err(format!("idx {} overflows", i)))?;
        Ok(Factor { op })
    }

    pub fn depth(&self) -> usize {
        self.op.depth()
    }

    pub fn children_indices(&self) -> Vec<usize> {
        self.op.children_indices()
    }

    pub fn symbols(&self) -> Vec<String> {
        self.op.symbols()
    }

    pub fn clone(&self) -> Factor {
        Factor {
            op: self.op.clone(),
        }
    }
}

#[pyproto]
impl PySequenceProtocol for Factor {
    fn __len__(&'p self) -> usize {
        self.op.len()
    }

    fn __getitem__(&'p self, idx: isize) -> PyResult<Factor> {
        if idx < 0 {
            throw!(PyValueError::new_err(format!("idx {} less than 0", idx)))
        }

        Ok(Factor {
            op: self
                .op
                .get(idx as usize)
                .ok_or_else(|| PyValueError::new_err(format!("idx {} overflows", idx)))?,
        })
    }
}

#[pyproto]
impl PyObjectProtocol for Factor {
    fn __str__(&self) -> PyResult<String> {
        Ok(self.op.to_string())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(self.op.to_string())
    }

    fn __hash__(&self) -> PyResult<u64> {
        let mut hasher = DefaultHasher::new();
        self.op.to_string().hash(&mut hasher);

        Ok(hasher.finish())
    }

    fn __richcmp__(&self, other: PyRef<Factor>, op: CompareOp) -> PyResult<bool> {
        let a = self.op.to_string();
        let b = other.op.to_string();
        Ok(match op {
            CompareOp::Eq => a == b,
            CompareOp::Ne => a != b,
            CompareOp::Le => a <= b,
            CompareOp::Lt => a < b,
            CompareOp::Ge => a >= b,
            CompareOp::Gt => a > b,
        })
    }
}

#[pyfunction]
pub fn replay<'py>(
    py: Python<'py>,
    file: &str,
    mut ops: Vec<Py<Factor>>,
    batch_size: Option<usize>,
) -> PyResult<ReplayResult> {
    let mut ops: Vec<_> = ops.iter_mut().map(|f| f.borrow_mut(py)).collect();
    let ops = ops
        .iter_mut()
        .map(|f| (&mut *f.op) as &mut dyn Operator<RecordBatch>)
        .collect();

    let (index, succeeded, failed) = py.allow_threads(|| {
        super::replay::replay(file, ops, batch_size)
            .map_err(|e| PyValueError::new_err(format!("{}", e)))
    })?;

    let (p1, p2) = index.to_raw().unwrap();
    Ok(ReplayResult {
        index: (p1 as usize, p2 as usize),
        succeeded: succeeded
            .into_iter()
            .map(|(k, v)| {
                let (p1, p2) = v.to_raw().unwrap();
                (k, (p1 as usize, p2 as usize))
            })
            .collect(),
        failed: failed
            .into_iter()
            .map(|(k, v)| (k, format!("{}", v)))
            .collect(),
    })
}
