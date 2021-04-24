use ndarray::Array1;
use parquet::column::reader::ColumnReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::mem::transmute;
use std::{fs::File, path::Path};

pub struct Tickers {
    pub time: Array1<u64>,
    pub bids: Array1<f64>,
    pub asks: Array1<f64>,
}

pub fn read_tickers() -> Tickers {
    let tickers = File::open(&Path::new("tickers.pq")).unwrap();
    let reader = SerializedFileReader::new(tickers).unwrap();
    let metadata = reader.metadata();
    let fmetadata = metadata.file_metadata();

    let nrows = fmetadata.num_rows() as usize;
    let ncols = reader.get_row_group(0).unwrap().num_columns() as usize;

    let mut tickers = Tickers {
        time: Array1::default((nrows,)),
        bids: Array1::default((nrows,)),
        asks: Array1::default((nrows,)),
    };

    let mut i = 0;
    for r in 0..reader.num_row_groups() {
        let rg = reader.get_row_group(r).unwrap();
        let nrgrows = rg.metadata().num_rows() as usize;

        for j in 0..ncols {
            match fmetadata.schema_descr().column(j).name() {
                "bid1" => {
                    if let ColumnReader::DoubleColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = tickers.bids.as_slice_mut().unwrap();

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                "ask1" => {
                    if let ColumnReader::DoubleColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = tickers.asks.as_slice_mut().unwrap();

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                "__index_level_0__" => match rg.get_column_reader(j).unwrap() {
                    ColumnReader::Int64ColumnReader(mut rdr) => {
                        let view = tickers.time.as_slice_mut().unwrap();
                        let view: &mut [i64] = unsafe { transmute(view) };

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    }

                    _ => todo!("6"),
                },
                name => panic!("unexpected name {}", name),
            }
        }

        i += nrgrows;
    }
    tickers
}

pub struct Signals {
    pub time: Array1<u64>,
    pub direction: Array1<i64>,
    pub takeprofit: Array1<f64>,
    pub stoploss: Array1<f64>,
    pub expiry: Array1<u64>,
}

pub fn read_signals() -> Signals {
    let signals = File::open(&Path::new("signals.pq")).unwrap();
    let reader = SerializedFileReader::new(signals).unwrap();
    let metadata = reader.metadata();
    let fmetadata = metadata.file_metadata();
    let nrows = fmetadata.num_rows() as usize;
    let ncols = reader.get_row_group(0).unwrap().num_columns() as usize;

    let mut signals = Signals {
        time: Array1::default((nrows,)),
        direction: Array1::default((nrows,)),
        takeprofit: Array1::default((nrows,)),
        stoploss: Array1::default((nrows,)),
        expiry: Array1::default((nrows,)),
    };

    let mut i = 0;
    for r in 0..reader.num_row_groups() {
        let rg = reader.get_row_group(r).unwrap();
        let nrgrows = rg.metadata().num_rows() as usize;

        for j in 0..ncols {
            match fmetadata.schema_descr().column(j).name() {
                "__index_level_0__" => {
                    if let ColumnReader::Int64ColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = signals.time.as_slice_mut().unwrap();
                        let view: &mut [i64] = unsafe { transmute(view) };

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                "direction" => {
                    if let ColumnReader::Int64ColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = signals.direction.as_slice_mut().unwrap();

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                "takeprofit" => {
                    if let ColumnReader::DoubleColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = signals.takeprofit.as_slice_mut().unwrap();

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                "stoploss" => {
                    if let ColumnReader::DoubleColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = signals.stoploss.as_slice_mut().unwrap();

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                "expiry" => {
                    if let ColumnReader::Int64ColumnReader(mut rdr) =
                        rg.get_column_reader(j).unwrap()
                    {
                        let view = signals.expiry.as_slice_mut().unwrap();
                        let view: &mut [i64] = unsafe { transmute(view) };

                        let (n, _) = rdr
                            .read_batch(nrgrows, None, None, &mut view[i..i + nrgrows])
                            .unwrap();
                        assert_eq!(n, nrgrows);
                    } else {
                        panic!()
                    }
                }
                name => panic!("unexpected name {}", name),
            }
        }

        i += nrgrows;
    }
    signals
}
