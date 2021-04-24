use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::mem;

pub struct TSLogReturn<T> {
    win_size: usize,
    s: BoxOp<T>,

    cache: VecDeque<f64>,
    warmup: usize,
}

impl<T> Clone for TSLogReturn<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.s.clone())
    }
}

impl<T> TSLogReturn<T> {
    pub fn new(win_size: usize, s: BoxOp<T>) -> Self {
        Self {
            win_size,
            s,
            cache: VecDeque::new(),
            warmup: 0,
        }
    }
}

impl<T> Named for TSLogReturn<T> {
    const NAME: &'static str = "TSLogReturn";
}

impl<T: TickerBatch> Operator<T> for TSLogReturn<T> {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        let ss = &*self.s.update(tb)?;

        let mut results = Vec::with_capacity(ss.len());

        let mut i = 0;
        while i + self.warmup < self.s.ready_offset() && i < ss.len() {
            results.push(f64::NAN);
            i += 1;
        }

        while i + self.warmup < self.ready_offset() && i < ss.len() {
            // maintain
            self.cache.push_back(ss[i]);

            results.push(f64::NAN);
            i += 1;
        }
        self.warmup += i;

        for i in i..ss.len() {
            let val = ss[i];
            // maintain
            self.cache.push_back(val);

            // Compute
            let result = (self.cache.back().unwrap() / self.cache.front().unwrap()).ln();
            results.push(self.fchecked(result)?);

            // maintain
            self.cache.pop_front();
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        self.s.ready_offset() + self.win_size - 1
    }

    fn to_string(&self) -> String {
        format!("({} {} {})", Self::NAME, self.win_size, self.s.to_string())
    }

    fn depth(&self) -> usize {
        1 + self.s.depth()
    }

    fn len(&self) -> usize {
        self.s.len() + 1
    }

    fn subindices(&self) -> Vec<usize> {
        vec![1]
    }

    fn symbols(&self) -> Vec<String> {
        self.s.symbols()
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i == 0 {
            return self.clone().boxed();
        }
        let i = i - 1;

        let ns = self.s.len();

        if i < ns {
            self.s.get(i)?
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

        let ns = self.s.len();

        if i < ns {
            if i == 0 {
                return mem::replace(&mut self.s, op) as BoxOp<T>;
            }
            self.s.insert(i, op)?
        } else {
            throw!()
        }
    }
}

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<TSLogReturn<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> TSLogReturn<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 2 {
            throw!(anyhow!(
                "{} expect a constant and a series, got {:?}",
                TSLogReturn::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0);
        match (k1, k2) {
            (Parameter::Constant(c), Parameter::Operator(s)) => TSLogReturn::new(c as usize, s),
            (a, b) => throw!(anyhow!(
                "{name} expect a constant and a series, got ({name} {} {})",
                a,
                b,
                name = TSLogReturn::<T>::NAME,
            )),
        }
    }
}
