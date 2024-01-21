use crate::ops::Operator;
use anyhow::{Error, Result};
use arrow::{
    array::{Float64Array, Float64Builder},
    record_batch::RecordBatch,
};
use fehler::throws;
use parquet::file::reader::SerializedFileReader;
use parquet::{arrow::arrow_reader::ParquetRecordBatchReader, file::reader::FileReader};
use rayon::prelude::*;
use std::fs::File;
use std::{borrow::Cow, collections::HashMap};

static DEFAULT_BATCH_SIZE: usize = 2048;

#[throws(Error)]
pub fn replay<'a, I>(
    tb: I,
    mut ops: Vec<&mut (dyn Operator<RecordBatch>)>,
    nrows: Option<usize>,
) -> (HashMap<usize, Float64Array>, HashMap<usize, Error>)
where
    I: IntoIterator<Item = Cow<'a, RecordBatch>>,
{
    let mut failed = HashMap::new();

    let mut builders: Vec<_> = (0..ops.len())
        .into_par_iter()
        .map(|_| {
            if let Some(nrows) = nrows {
                Float64Builder::with_capacity(nrows)
            } else {
                Float64Builder::new()
            }
        })
        .collect();

    for record_batch in tb {
        let results: Vec<_> = ops
            .par_iter_mut()
            .zip(&mut builders)
            .enumerate()
            .map(|(i, (op, bdr))| -> Result<()> {
                if failed.contains_key(&i) {
                    return Ok(());
                }
                let values = op.update(&record_batch)?;
                let masks: Vec<_> = values.iter().map(|v| !v.is_nan()).collect();
                bdr.append_values(&values, &masks);

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
        builders
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !failed.contains_key(&i))
            .map(|(i, mut bdr)| (i, bdr.finish()))
            .collect(),
        failed,
    )
}

#[throws(Error)]
pub fn replay_file<O>(
    path: &str,
    ops: Vec<&mut (dyn Operator<RecordBatch>)>,
    batch_size: O,
) -> (usize, HashMap<usize, Float64Array>, HashMap<usize, Error>)
where
    O: Into<Option<usize>>,
{
    let file = File::open(path).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let nrows: usize = file_reader
        .metadata()
        .row_groups()
        .into_iter()
        .map(|rgm| rgm.num_rows() as usize)
        .sum();

    let file = File::open(path).unwrap();
    let batch_size = batch_size.into().unwrap_or(DEFAULT_BATCH_SIZE);
    let arrow_reader = ParquetRecordBatchReader::try_new(file, batch_size)?;

    // let schema = arrow_reader.get_schema()?;
    // // Only read columns that we used
    // let column_indices = ops
    //     .iter()
    //     .flat_map(|op| op.columns())
    //     .map(|sym| schema.index_of(&sym))
    //     .collect::<Result<HashSet<usize>, _>>()?;

    // let record_batch_reader = arrow_reader
    //     .get_record_reader_by_columns(
    //         column_indices,
    //         batch_size.into().unwrap_or(DEFAULT_BATCH_SIZE),
    //     )
    //     .unwrap();

    let (succeeded, failed) = replay(
        arrow_reader
            .into_iter()
            .filter_map(|b| b.ok())
            .map(Cow::Owned),
        ops,
        Some(nrows),
    )?;

    (nrows, succeeded, failed)
}
