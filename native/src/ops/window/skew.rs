use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::mem;
use std::{collections::VecDeque, iter::FromIterator};

pub struct TSSkew<T> {
    win_size: usize,
    inner: BoxOp<T>,

    window: VecDeque<f64>,
    sum: f64,
    i: usize,
}

impl<T> Clone for TSSkew<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.inner.clone())
    }
}

impl<T> TSSkew<T> {
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

impl<T> Named for TSSkew<T> {
    const NAME: &'static str = "TSSkew";
}

impl<T: TickerBatch> Operator<T> for TSSkew<T> {
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
                let m3 = self.window.iter().map(|x| (x - mu).powf(3.0)).sum::<f64>() / n;
                let m2 = self.window.iter().map(|x| (x - mu).powf(2.0)).sum::<f64>() / n;

                let val = if m2 == 0. {
                    0.
                } else {
                    // do not use window function because this will overflow
                    // let m3 =
                    //     cache.xxx / n - 3. / n / n * cache.xx * cache.x + 2. / n.powf(3.) * cache.x.powf(3.);
                    // let m2 = cache.xx / n - cache.x * cache.x / n / n;
                    let correction = (n * (n - 1.)).sqrt() / (n - 2.);
                    let result = correction * m3 / m2.powf(1.5);

                    self.fchecked(result)?
                };

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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<TSSkew<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> TSSkew<T> {
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
            (Parameter::Constant(c), Parameter::Operator(s)) if c >= 3. => {
                TSSkew::new(c as usize, s)
            }
            (Parameter::Constant(c), Parameter::Operator(_)) if c < 3. => {
                throw!(anyhow!(
                    "{} for requires constant larger than 2, got {}",
                    TSSkew::<T>::NAME,
                    c
                ))
            }
            (a, b) => throw!(anyhow!(
                "{name} expect a constant and a series, got ({name} {} {})",
                a,
                b,
                name = TSSkew::<T>::NAME,
            )),
        }
    }
}
