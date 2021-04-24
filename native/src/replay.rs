use crate::ops::Operator;
use anyhow::{Error, Result};
use arrow::{
    array::{
        as_primitive_array, Float64Array, Float64Builder, Int64Array, Int64Builder,
        TimestampMillisecondArray,
    },
    record_batch::RecordBatch,
};
use fehler::throws;
use parquet::file::reader::SerializedFileReader;
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::reader::FileReader,
};
use rayon::prelude::*;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::sync::Arc;

static DEFAULT_BATCH_SIZE: usize = 2048;

#[throws(Error)]
pub fn replay<O>(
    file: &str,
    mut ops: Vec<&mut (dyn Operator<RecordBatch>)>,
    batch_size: O,
) -> (
    Int64Array,
    BTreeMap<usize, Float64Array>,
    BTreeMap<usize, Error>,
)
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
    let mut column_indices = ops
        .iter()
        .flat_map(|op| op.symbols())
        .map(|sym| schema.index_of(&sym))
        .collect::<Result<HashSet<usize>, _>>()?;
    column_indices.insert(schema.index_of("time")?);

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
    let mut index_builder = Int64Builder::new(nrows);

    let mut failed = BTreeMap::new();

    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();

        let schema = record_batch.schema();
        let timecol = record_batch.column(schema.index_of("time")?);
        let timecol: &TimestampMillisecondArray = as_primitive_array(&timecol);
        index_builder.append_slice(timecol.values())?;

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
        index_builder.finish(),
        builders
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !failed.contains_key(&i))
            .map(|(i, mut bdr)| (i, bdr.finish()))
            .collect(),
        failed,
    )
}
