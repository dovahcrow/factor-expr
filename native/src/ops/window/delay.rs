use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::{borrow::Cow, collections::VecDeque, iter::FromIterator, mem};

pub struct Delay<T> {
    win_size: usize,
    inner: BoxOp<T>,

    window: VecDeque<f64>,
    i: usize,
}

impl<T> Clone for Delay<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.inner.clone())
    }
}

impl<T> Delay<T> {
    pub fn new(win_size: usize, inner: BoxOp<T>) -> Self {
        Self {
            win_size,
            inner,
            window: VecDeque::with_capacity(win_size + 1),
            i: 0,
        }
    }
}

impl<T> Named for Delay<T> {
    const NAME: &'static str = "Delay";
}

impl<T: TickerBatch> Operator<T> for Delay<T> {
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

            let val = if self.window.len() == self.win_size + 1 {
                let result = self.window.pop_front().unwrap();
                self.fchecked(result)?
            } else {
                f64::NAN
            };
            results.push(val);
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        self.inner.ready_offset() + self.win_size
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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<Delay<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> Delay<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 2 {
            throw!(anyhow!(
                "{} expect a constant and a series, got {:?}",
                Delay::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0);
        match (k1, k2) {
            (Parameter::Constant(c), Parameter::Operator(s)) => Delay::new(c as usize, s),
            (a, b) => throw!(anyhow!(
                "{name} expect a constant and a series, got ({name} {} {})",
                a,
                b,
                name = Delay::<T>::NAME,
            )),
        }
    }
}
