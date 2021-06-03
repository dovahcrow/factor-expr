use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::float::{Ascending, Float, IntoFloat};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use order_stats_tree::OSTree;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::mem;

#[derive(Clone)]
struct Cache {
    history: VecDeque<f64>,
    ostree: OSTree<Float<Ascending>>, // sorted window
}

impl Cache {
    pub fn new() -> Cache {
        Cache {
            history: VecDeque::new(),
            ostree: OSTree::new(),
        }
    }
}

pub struct TSQuantile<T> {
    win_size: usize,
    quantile: f64,
    r: usize, // win_size * quantile
    s: BoxOp<T>,

    cache: Cache,
    warmup: usize,
}

impl<T> Clone for TSQuantile<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.quantile, self.s.clone())
    }
}

impl<T> TSQuantile<T> {
    pub fn new(win_size: usize, quantile: f64, s: BoxOp<T>) -> Self {
        assert!(0. <= quantile && quantile <= 1.);
        Self {
            win_size,
            s,
            quantile,
            r: ((win_size - 1) as f64 * quantile).floor() as usize,
            cache: Cache::new(),
            warmup: 0,
        }
    }
}

impl<T> Named for TSQuantile<T> {
    const NAME: &'static str = "TSQuantile";
}

impl<T: TickerBatch> Operator<T> for TSQuantile<T> {
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
            let val = ss[i];

            self.cache.history.push_back(val);
            self.cache.ostree.increase(val.asc(), 1);

            results.push(f64::NAN);
            i += 1;
        }
        self.warmup += i;

        for i in i..ss.len() {
            let val = ss[i];

            // maintain
            self.cache.history.push_back(val);
            self.cache.ostree.increase(val.asc(), 1);

            // compute
            let (v, _) = self.cache.ostree.select(self.r).unwrap();
            results.push(self.fchecked(v.0)?);

            // maintain
            let to_remove = self.cache.history.pop_front().unwrap().asc();
            self.cache.ostree.decrease(&to_remove, 1);
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        self.s.ready_offset() + self.win_size - 1
    }

    fn to_string(&self) -> String {
        format!(
            "({} {} {} {})",
            Self::NAME,
            self.win_size,
            self.quantile,
            self.s.to_string(),
        )
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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<TSQuantile<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> TSQuantile<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 3 {
            throw!(anyhow!(
                "{} expect two constants and one series, got {:?}",
                TSQuantile::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0);
        let k3 = params.remove(0);
        match (k1, k2, k3) {
            (Parameter::Constant(c), Parameter::Constant(c2), Parameter::Operator(s)) => {
                TSQuantile::new(c as usize, c2, s)
            }
            (a, b, c) => throw!(anyhow!(
                "{name} expect two constants and a series, got ({name} {} {} {})",
                a,
                b,
                c,
                name = TSQuantile::<T>::NAME,
            )),
        }
    }
}
