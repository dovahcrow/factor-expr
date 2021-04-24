use crate::ops::Operator;
use anyhow::{Error, Result};
use arrow::{
    array::{Float64Array, Float64Builder},
    record_batch::RecordBatch,
};
use fehler::throws;
use parquet::file::reader::SerializedFileReader;
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::reader::FileReader,
};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;

static DEFAULT_BATCH_SIZE: usize = 2048;

#[throws(Error)]
pub fn replay<O>(
    file: &str,
    mut ops: Vec<&mut (dyn Operator<RecordBatch>)>,
    batch_size: O,
) -> (usize, HashMap<usize, Float64Array>, HashMap<usize, Error>)
where
    O: Into<Option<usize>>,
{
    let file = File::open(file).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let nrows: usize = file_reader
        .metadata()
        .row_groups()
        .into_iter()
        .map(|rgm| rgm.num_rows() as usize)
        .sum();

    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    let schema = arrow_reader.get_schema()?;
    // Only read columns that we used
    let column_indices = ops
        .iter()
        .flat_map(|op| op.columns())
        .map(|sym| schema.index_of(&sym))
        .collect::<Result<HashSet<usize>, _>>()?;

    let record_batch_reader = arrow_reader
        .get_record_reader_by_columns(
            column_indices,
            batch_size.into().unwrap_or(DEFAULT_BATCH_SIZE),
        )
        .unwrap();

    let mut builders: Vec<_> = (0..ops.len())
        .into_par_iter()
        .map(|_| Float64Builder::new(nrows))
        .collect();

    let mut failed = HashMap::new();

    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();

        let results: Vec<_> = ops
            .par_iter_mut()
            .zip(&mut builders)
            .enumerate()
            .map(|(i, (op, bdr))| -> Result<()> {
                if failed.contains_key(&i) {
                    return Ok(());
                }
                bdr.append_slice(&op.update(&record_batch)?)?;

                Ok(())
            })
            .collect();

        for (i, result) in results.into_iter().enumerate() {
            if let Err(e) = result {
                failed.insert(i, e);
            }
        }
    }

    (
        nrows,
        builders
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !failed.contains_key(&i))
            .map(|(i, mut bdr)| (i, bdr.finish()))
            .collect(),
        failed,
    )
}
