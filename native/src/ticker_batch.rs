use arrow::{
    array::{as_primitive_array, Float64Array},
    record_batch::RecordBatch,
};
use std::collections::HashMap;

// Tickers should be sync because we will do parallel replay
pub trait TickerBatch: Sync + 'static {
    fn index_of(&self, name: &str) -> Option<usize>;
    fn values<'a>(&'a self, i: usize) -> Option<&'a [f64]>;
    fn len(&self) -> usize;
}

impl TickerBatch for RecordBatch {
    fn index_of(&self, name: &str) -> Option<usize> {
        let schema = self.schema();
        schema.index_of(name).ok()
    }

    fn values(&self, i: usize) -> Option<&[f64]> {
        let col = self.column(i);
        let col: &Float64Array = as_primitive_array(col);
        Some(col.values())
    }

    fn len(&self) -> usize {
        self.num_rows()
    }
}

pub struct SingleRow {
    schema: HashMap<String, usize>,
    data: Vec<f64>,
}

impl TickerBatch for SingleRow {
    fn index_of(&self, name: &str) -> Option<usize> {
        self.schema.get(name).cloned()
    }

    fn values(&self, i: usize) -> Option<&[f64]> {
        Some(&self.data[i..i + 1])
    }

    fn len(&self) -> usize {
        1
    }
}
