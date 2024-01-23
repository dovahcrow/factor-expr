use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::{
    float::{Ascending, Float, IntoFloat},
    ticker_batch::TickerBatch,
};
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use order_stats_tree::OSTree;
use std::{borrow::Cow, collections::VecDeque, iter::FromIterator, mem};

pub struct Rank<T> {
    win_size: usize,
    inner: BoxOp<T>,

    window: VecDeque<f64>,
    ostree: OSTree<Float<Ascending>>, // sorted window
    i: usize,
}

impl<T> Clone for Rank<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.inner.clone())
    }
}

impl<T> Rank<T> {
    pub fn new(win_size: usize, inner: BoxOp<T>) -> Self {
        Self {
            win_size,
            inner,

            window: VecDeque::with_capacity(win_size),
            ostree: OSTree::new(),
            i: 0,
        }
    }
}

impl<T> Named for Rank<T> {
    const NAME: &'static str = "Rank";
}

impl<T: TickerBatch> Operator<T> for Rank<T> {
    fn reset(&mut self) {
        self.inner.reset();
        self.window.clear();
        self.ostree.clear();
        self.i = 0;
    }

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
            self.ostree.increase(val.asc(), 1);
            let val = if self.window.len() == self.win_size {
                let idx = self.ostree.rank(&val.asc()).unwrap();
                let val = self.fchecked(idx as f64)?;

                let to_remove = self.window.pop_front().unwrap().asc();
                self.ostree.decrease(&to_remove, 1);

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
            self.inner.to_string(),
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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<Rank<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> Rank<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 2 {
            throw!(anyhow!(
                "{} expect a constant and one series, got {:?}",
                Rank::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0);
        match (k1, k2) {
            (Parameter::Constant(c), Parameter::Operator(s)) => Rank::new(c as usize, s),
            (a, b) => throw!(anyhow!(
                "{name} expect a constant and a series, got ({name} {} {})",
                a,
                b,
                name = Rank::<T>::NAME,
            )),
        }
    }
}
