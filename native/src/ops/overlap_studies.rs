use std::{borrow::Cow, collections::VecDeque, iter::FromIterator, mem};

use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};

use crate::ticker_batch::TickerBatch;

use super::{parser::Parameter, BoxOp, Named, Operator};

pub struct SMA<T> {
    inner: BoxOp<T>,
    win_size: usize,

    i: usize,
    window: VecDeque<f64>,
    sum: f64,
}

impl<T> Clone for SMA<T> {
    fn clone(&self) -> Self {
        Self::new(self.inner.clone(), self.win_size)
    }
}

impl<T> SMA<T> {
    pub fn new(inner: BoxOp<T>, n: usize) -> Self {
        Self {
            window: VecDeque::with_capacity(n),
            sum: 0.,
            i: 0,

            inner,
            win_size: n,
        }
    }
}

impl<T> Named for SMA<T> {
    const NAME: &'static str = "SMA";
}

impl<T: TickerBatch> Operator<T> for SMA<T> {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        let vals = &*self.inner.update(tb)?;
        #[cfg(feature = "check")]
        assert_eq!(tb.len(), vals.len());

        let mut results = Vec::with_capacity(tb.len());

        for &val in vals {
            if self.i < self.inner.ready_offset() {
                #[cfg(feature = "check")]
                assert!(val.is_nan());
                results.push(f64::NAN);
                self.i += 1;
                continue;
            }

            self.window.push_back(val);
            self.sum += val;
            let val = if self.window.len() == self.win_size {
                let val = self.sum / self.win_size as f64;
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
            self.win_size.to_string(),
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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<SMA<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> SMA<T> {
        let mut iter = iter.into_iter();

        let Parameter::Constant(n) = iter.next().unwrap() else {
            throw!(anyhow!("<n> for SMA should be an constant"));
        };

        let inner = iter
            .next()
            .unwrap()
            .to_operator()
            .ok_or_else(|| anyhow!("<inner> for SMA should be an operator"))?;

        if iter.count() != 0 {
            throw!(anyhow!("Too many parameters for SMA"))
        }

        SMA::new(inner, n as usize)
    }
}
