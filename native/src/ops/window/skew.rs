use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::mem;
use std::{collections::VecDeque, iter::FromIterator};

#[derive(Clone)]
struct Cache {
    history: VecDeque<f64>,
    x: f64,
}

impl Cache {
    pub fn new() -> Cache {
        Cache {
            history: VecDeque::new(),
            x: 0.,
        }
    }
}

pub struct TSSkew<T> {
    win_size: usize,
    s: BoxOp<T>,

    cache: Cache,
    warmup: usize,
}

impl<T> Clone for TSSkew<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.s.clone())
    }
}

impl<T> TSSkew<T> {
    pub fn new(win_size: usize, s: BoxOp<T>) -> Self {
        Self {
            win_size,
            s,

            cache: Cache::new(),
            warmup: 0,
        }
    }
}

impl<T> Named for TSSkew<T> {
    const NAME: &'static str = "TSSkew";
}

impl<T: TickerBatch> Operator<T> for TSSkew<T> {
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
            self.cache.history.push_back(ss[i]);
            self.cache.x += ss[i];

            results.push(f64::NAN);
            i += 1;
        }
        self.warmup += i;

        for i in i..ss.len() {
            let val = ss[i];
            // maintain
            self.cache.history.push_back(val);
            self.cache.x += val;

            // compute
            let n = self.cache.history.len() as f64;
            let mu = self.cache.x / n;
            let m3 = self
                .cache
                .history
                .iter()
                .map(|x| (x - mu).powf(3.0))
                .sum::<f64>()
                / n;
            let m2 = self
                .cache
                .history
                .iter()
                .map(|x| (x - mu).powf(2.0))
                .sum::<f64>()
                / n;

            if m2 == 0. {
                results.push(0.);
            } else {
                // do not use window function because this will overflow
                // let m3 =
                //     cache.xxx / n - 3. / n / n * cache.xx * cache.x + 2. / n.powf(3.) * cache.x.powf(3.);
                // let m2 = cache.xx / n - cache.x * cache.x / n / n;
                let correction = (n * (n - 1.)).sqrt() / (n - 2.);
                let result = correction * m3 / m2.powf(1.5);

                results.push(self.fchecked(result)?);
            }

            // maintain
            self.cache.x -= self.cache.history.pop_front().unwrap();
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

    fn child_indices(&self) -> Vec<usize> {
        vec![1]
    }

    fn columns(&self) -> Vec<String> {
        self.s.columns()
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
