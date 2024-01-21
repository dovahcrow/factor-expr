use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::mem;
use std::{collections::VecDeque, iter::FromIterator};

pub struct TSStdev<T> {
    win_size: usize,
    inner: BoxOp<T>,

    window: VecDeque<f64>,
    sum: f64,
    i: usize,
}

impl<T> Clone for TSStdev<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.inner.clone())
    }
}

impl<T> TSStdev<T> {
    pub fn new(win_size: usize, inner: BoxOp<T>) -> Self {
        Self {
            win_size,
            inner,

            window: VecDeque::with_capacity(win_size),
            sum: 0.,
            i: 0,
        }
    }
}

impl<T> Named for TSStdev<T> {
    const NAME: &'static str = "TSStd";
}

impl<T: TickerBatch> Operator<T> for TSStdev<T> {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        let vals = &*self.inner.update(tb)?;

        assert_eq!(tb.len(), vals.len());

        let mut results = Vec::with_capacity(tb.len());

        for &val in vals {
            if self.i < self.inner.ready_offset() {
                results.push(f64::NAN);
                self.i += 1;
                continue;
            }

            self.window.push_back(val);
            self.sum += val;
            let val = if self.window.len() == self.win_size {
                let n = self.window.len() as f64;
                let mu = self.sum / n;
                let sum = self.window.iter().map(|v| (v - mu).powf(2.)).sum::<f64>();

                let result = (sum / (n - 1.)).sqrt();

                let val = self.fchecked(result)?;

                self.sum -= self.window.pop_front().unwrap();

                val
            } else {
                f64::NAN
            };

            results.push(val);
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        self.inner.ready_offset() + self.win_size - 1
    }

    fn to_string(&self) -> String {
        format!(
            "({} {} {})",
            Self::NAME,
            self.win_size,
            self.inner.to_string()
        )
    }

    fn depth(&self) -> usize {
        1 + self.inner.depth()
    }

    fn len(&self) -> usize {
        self.inner.len() + 1
    }

    fn child_indices(&self) -> Vec<usize> {
        vec![1]
    }

    fn columns(&self) -> Vec<String> {
        self.inner.columns()
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i == 0 {
            return self.clone().boxed();
        }
        let i = i - 1;

        let ns = self.inner.len();

        if i < ns {
            self.inner.get(i)?
        } else {
            throw!()
        }
    }

    #[throws(as Option)]
    fn insert(&mut self, i: usize, op: BoxOp<T>) -> BoxOp<T> {
        if i == 0 {
            unreachable!("cannot insert root");
        }
        let i = i - 1;

        let ns = self.inner.len();

        if i < ns {
            if i == 0 {
                return mem::replace(&mut self.inner, op) as BoxOp<T>;
            }
            self.inner.insert(i, op)?
        } else {
            throw!()
        }
    }
}

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<TSStdev<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> TSStdev<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 2 {
            throw!(anyhow!(
                "{} expect two series, got {:?}",
                stringify!($op),
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0);
        match (k1, k2) {
            (Parameter::Constant(c), Parameter::Operator(s)) => {
                if c <= 1. {
                    throw!(anyhow!(
                        "win size for {} should larger than 1",
                        TSStdev::<T>::NAME
                    ))
                }
                TSStdev::new(c as usize, s)
            }
            (a, b) => throw!(anyhow!(
                "{name} expect a constant and a series, got ({name} {} {})",
                a,
                b,
                name = TSStdev::<T>::NAME,
            )),
        }
    }
}
