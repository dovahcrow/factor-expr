use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::cmp::max;
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::mem;

#[derive(Clone)]
struct Cache {
    history: VecDeque<(f64, f64)>,

    x: f64,
    y: f64,
}

impl Cache {
    fn new() -> Cache {
        Cache {
            history: VecDeque::new(),

            x: 0.,
            y: 0.,
        }
    }
}

pub struct TSCorrelation<T> {
    win_size: usize,
    sx: BoxOp<T>,
    sy: BoxOp<T>,

    cache: Cache,
    warmup: usize,
}

impl<T> Clone for TSCorrelation<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.sx.clone(), self.sy.clone())
    }
}

impl<T> TSCorrelation<T> {
    pub fn new(win_size: usize, x: BoxOp<T>, y: BoxOp<T>) -> Self {
        Self {
            win_size,
            sx: x,
            sy: y,

            cache: Cache::new(),
            warmup: 0,
        }
    }
}

impl<T> Named for TSCorrelation<T> {
    const NAME: &'static str = "TSCorr";
}

impl<T: TickerBatch> Operator<T> for TSCorrelation<T> {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        let (sx, sy) = (&mut self.sx, &mut self.sy);
        let (xs, ys) = rayon::join(|| sx.update(tb), || sy.update(tb));
        let (xs, ys) = (xs?, ys?);

        let mut results = Vec::with_capacity(xs.len());

        let mut i = 0;
        while i + self.warmup < max(self.sx.ready_offset(), self.sy.ready_offset()) && i < xs.len()
        {
            results.push(f64::NAN);
            i += 1;
        }

        while i + self.warmup < self.ready_offset() && i < xs.len() {
            // maintain
            let (xval, yval) = (xs[i], ys[i]);

            self.cache.history.push_back((xval, yval));
            self.cache.x += xval;
            self.cache.y += yval;

            results.push(f64::NAN);
            i += 1;
        }
        self.warmup += i;

        for i in i..xs.len() {
            let (xval, yval) = (xs[i], ys[i]);

            // maintain
            self.cache.history.push_back((xval, yval));
            self.cache.x += xval;
            self.cache.y += yval;

            // compute
            let n = self.cache.history.len() as f64; // this should be equal to self.win_size
            let xbar = self.cache.x / n;
            let ybar = self.cache.y / n;
            let nom = self
                .cache
                .history
                .iter()
                .map(|(x, y)| (x - xbar) * (y - ybar))
                .sum::<f64>();
            let denomx = self
                .cache
                .history
                .iter()
                .map(|(x, _)| (x - xbar).powf(2.))
                .sum::<f64>()
                .sqrt();
            let denomy = self
                .cache
                .history
                .iter()
                .map(|(_, y)| (y - ybar).powf(2.))
                .sum::<f64>()
                .sqrt();

            let denom = denomx * denomy;

            if denom == 0. {
                results.push(0.);
            } else {
                results.push(self.fchecked(nom / denom)?);
            }

            // maintain
            let (xval, yval) = self.cache.history.pop_front().unwrap();
            self.cache.x -= xval;
            self.cache.y -= yval;
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        max(self.sx.ready_offset(), self.sy.ready_offset()) + self.win_size - 1
    }

    fn to_string(&self) -> String {
        format!(
            "({} {} {} {})",
            Self::NAME,
            self.win_size,
            self.sx.to_string(),
            self.sy.to_string()
        )
    }

    fn depth(&self) -> usize {
        1 + max(self.sx.depth(), self.sy.depth())
    }

    fn len(&self) -> usize {
        self.sx.len() + self.sy.len() + 1
    }

    fn child_indices(&self) -> Vec<usize> {
        vec![1, self.sx.len() + 1]
    }

    fn columns(&self) -> Vec<String> {
        self.sx
            .columns()
            .into_iter()
            .chain(self.sy.columns())
            .collect()
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i == 0 {
            return self.clone().boxed();
        }
        let i = i - 1;

        let nx = self.sx.len();
        let ny = self.sy.len();

        if i < nx {
            self.sx.get(i)?
        } else if i >= nx && i < nx + ny {
            self.sy.get(i - nx)?
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

        let nx = self.sx.len();
        let ny = self.sy.len();

        if i < nx {
            if i == 0 {
                return mem::replace(&mut self.sx, op) as BoxOp<T>;
            }
            self.sx.insert(i, op)?
        } else if i >= nx && i < nx + ny {
            if i - nx == 0 {
                return mem::replace(&mut self.sy, op) as BoxOp<T>;
            }
            self.sy.insert(i - nx, op)?
        } else {
            throw!()
        }
    }
}

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<TSCorrelation<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> TSCorrelation<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 3 {
            throw!(anyhow!(
                "{} expect a constant and two series, got {:?}",
                TSCorrelation::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0).to_operator();
        let k3 = params.remove(0).to_operator();
        match (k1, k2, k3) {
            (Parameter::Constant(c), Some(sx), Some(sy)) => TSCorrelation::new(c as usize, sx, sy),
            _ => throw!(anyhow!(
                "{} expect a constant and two series",
                TSCorrelation::<T>::NAME,
            )),
        }
    }
}
