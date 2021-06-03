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

pub struct TSRank<T> {
    win_size: usize,
    s: BoxOp<T>,

    cache: Cache,
    warmup: usize,
}

impl<T> Clone for TSRank<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.s.clone())
    }
}

impl<T> TSRank<T> {
    pub fn new(win_size: usize, s: BoxOp<T>) -> Self {
        Self {
            win_size,
            s,

            cache: Cache::new(),
            warmup: 0,
        }
    }
}

impl<T> Named for TSRank<T> {
    const NAME: &'static str = "TSRank";
}

impl<T: TickerBatch> Operator<T> for TSRank<T> {
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
            let idx = self.cache.ostree.rank(&val.asc()).unwrap();
            results.push(self.fchecked(idx as f64)?);

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
        format!("({} {} {})", Self::NAME, self.win_size, self.s.to_string(),)
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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<TSRank<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> TSRank<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 2 {
            throw!(anyhow!(
                "{} expect a constant and two series, got {:?}",
                TSRank::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0);
        match (k1, k2) {
            (Parameter::Constant(c), Parameter::Operator(s)) => TSRank::new(c as usize, s),
            (a, b) => throw!(anyhow!(
                "{name} expect a constant and a series, got ({name} {} {})",
                a,
                b,
                name = TSRank::<T>::NAME,
            )),
        }
    }
}
