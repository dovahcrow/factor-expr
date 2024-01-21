use super::ops::{from_str, Operator};
use anyhow::Result;
use arrow::array::{make_array, Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{self, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use dict_derive::IntoPyObject;
use fehler::throw;
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

// *mut FFI_ArrowArray, *mut FFI_ArrowSchema
type ArrowFFIPtr = (usize, usize);

#[derive(IntoPyObject)]
pub struct ReplayResult {
    succeeded: HashMap<usize, ArrowFFIPtr>,
    failed: HashMap<usize, String>,
}

#[pyclass]
pub struct Factor {
    op: Box<dyn Operator<RecordBatch>>,
}

#[pymethods]
impl Factor {
    #[new]
    pub fn new(sexpr: &str) -> PyResult<Self> {
        Ok(Self {
            op: from_str(sexpr).map_err(|e| PyValueError::new_err(format!("{}", e)))?,
        })
    }

    pub fn ready_offset(&self) -> usize {
        self.op.ready_offset()
    }

    pub fn replace<'p>(&self, i: usize, other: PyRef<'p, Factor>) -> PyResult<Factor> {
        if i == 0 {
            return Ok(Factor {
                op: other.op.clone(),
            });
        }

        let mut op = self.op.clone();
        let _ = op
            .insert(i, other.op.clone())
            .ok_or_else(|| PyValueError::new_err(format!("idx {} overflows", i)))?;
        Ok(Factor { op })
    }

    pub fn depth(&self) -> usize {
        self.op.depth()
    }

    pub fn child_indices(&self) -> Vec<usize> {
        self.op.child_indices()
    }

    pub fn columns(&self) -> Vec<String> {
        self.op.columns()
    }

    pub fn clone(&self) -> Factor {
        Factor {
            op: self.op.clone(),
        }
    }

    fn __len__(&self) -> usize {
        self.op.len()
    }

    fn __getitem__(&self, idx: isize) -> PyResult<Factor> {
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
    schema: Vec<usize>,
    array: Vec<usize>,
    mut ops: Vec<Py<Factor>>,
    njobs: usize,
) -> PyResult<ReplayResult> {
    let mut ops: Vec<_> = ops.iter_mut().map(|f| f.borrow_mut(py)).collect();
    let ops = ops
        .iter_mut()
        .map(|f| (&mut *f.op) as &mut dyn Operator<RecordBatch>)
        .collect();

    let mut ffi_schemas = vec![];
    let mut fields = vec![];
    for schema in schema {
        let schema = unsafe { FFI_ArrowSchema::from_raw(schema as *mut _) };
        let dt = DataType::try_from(&schema)
            .map_err(|_| PyValueError::new_err("Cannot get data type"))?;
        let field = Field::new(schema.name(), dt, schema.nullable());
        fields.push(field);
        ffi_schemas.push(schema);
    }
    let schema = Arc::new(Schema::new(fields));

    let mut rbs = vec![];
    for rb in array.chunks_exact(schema.fields().len()) {
        let mut columns = vec![];

        for (&array, ffi_schema) in rb.into_iter().zip(&ffi_schemas) {
            let array = unsafe { FFI_ArrowArray::from_raw(array as *mut _) };
            let data = unsafe { ffi::from_ffi(array, ffi_schema).unwrap() };

            columns.push(make_array(data));
        }
        let rb = RecordBatch::try_new(schema.clone(), columns).unwrap();
        rbs.push(rb);
    }

    let (succeeded, failed) = py
        .allow_threads(|| -> Result<_> {
            let pool = rayon::ThreadPoolBuilder::new().num_threads(njobs).build()?;
            Ok(pool.install(|| crate::replay::replay(rbs.iter().map(Cow::Borrowed), ops, None))?)
        })
        .map_err(|e| PyValueError::new_err(format!("{}", e)))?;

    Ok(ReplayResult {
        succeeded: succeeded
            .into_iter()
            .map(|(k, v)| {
                let data = v.into_data();
                let (array, schema) = ffi::to_ffi(&data).unwrap();
                let array = Box::into_raw(Box::new(array));
                let schema = Box::into_raw(Box::new(schema));

                (k, (array as usize, schema as usize))
            })
            .collect(),
        failed: failed
            .into_iter()
            .map(|(k, v)| (k, format!("{}", v)))
            .collect(),
    })
}
