use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::{borrow::Cow, cmp::max, collections::VecDeque, iter::FromIterator, mem};

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

pub struct Correlation<T> {
    win_size: usize,
    x: BoxOp<T>,
    y: BoxOp<T>,

    cache: Cache,
    i: usize,
}

impl<T> Clone for Correlation<T> {
    fn clone(&self) -> Self {
        Self::new(self.win_size, self.x.clone(), self.y.clone())
    }
}

impl<T> Correlation<T> {
    pub fn new(win_size: usize, x: BoxOp<T>, y: BoxOp<T>) -> Self {
        Self {
            win_size,
            x,
            y,

            cache: Cache::new(),
            i: 0,
        }
    }
}

impl<T> Named for Correlation<T> {
    const NAME: &'static str = "Corr";
}

impl<T: TickerBatch> Operator<T> for Correlation<T> {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        let (x, y) = (&mut self.x, &mut self.y);
        let (xs, ys) = rayon::join(|| x.update(tb), || y.update(tb));
        let (xs, ys) = (&*xs?, &*ys?);
        #[cfg(feature = "check")]
        assert_eq!(tb.len(), xs.len());
        #[cfg(feature = "check")]
        assert_eq!(tb.len(), ys.len());

        let mut results = Vec::with_capacity(tb.len());

        for (&xval, &yval) in xs.into_iter().zip(ys) {
            if self.i < self.x.ready_offset() || self.i < self.y.ready_offset() {
                #[cfg(feature = "check")]
                assert!(xval.is_nan() || yval.is_nan());
                results.push(f64::NAN);
                self.i += 1;
                continue;
            }

            self.cache.history.push_back((xval, yval));
            self.cache.x += xval;
            self.cache.y += yval;

            let val = if self.cache.history.len() == self.win_size {
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

                let val = if denom == 0. {
                    0.
                } else {
                    self.fchecked(nom / denom)?
                };
                let (xval, yval) = self.cache.history.pop_front().unwrap();
                self.cache.x -= xval;
                self.cache.y -= yval;
                val
            } else {
                f64::NAN
            };

            results.push(val);
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        max(self.x.ready_offset(), self.y.ready_offset()) + self.win_size - 1
    }

    fn to_string(&self) -> String {
        format!(
            "({} {} {} {})",
            Self::NAME,
            self.win_size,
            self.x.to_string(),
            self.y.to_string()
        )
    }

    fn depth(&self) -> usize {
        1 + max(self.x.depth(), self.y.depth())
    }

    fn len(&self) -> usize {
        self.x.len() + self.y.len() + 1
    }

    fn child_indices(&self) -> Vec<usize> {
        vec![1, self.x.len() + 1]
    }

    fn columns(&self) -> Vec<String> {
        self.x
            .columns()
            .into_iter()
            .chain(self.y.columns())
            .collect()
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i == 0 {
            return self.clone().boxed();
        }
        let i = i - 1;

        let nx = self.x.len();
        let ny = self.y.len();

        if i < nx {
            self.x.get(i)?
        } else if i >= nx && i < nx + ny {
            self.y.get(i - nx)?
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

        let nx = self.x.len();
        let ny = self.y.len();

        if i < nx {
            if i == 0 {
                return mem::replace(&mut self.x, op) as BoxOp<T>;
            }
            self.x.insert(i, op)?
        } else if i >= nx && i < nx + ny {
            if i - nx == 0 {
                return mem::replace(&mut self.y, op) as BoxOp<T>;
            }
            self.y.insert(i - nx, op)?
        } else {
            throw!()
        }
    }
}

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<Correlation<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> Correlation<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 3 {
            throw!(anyhow!(
                "{} expect a constant and two series, got {:?}",
                Correlation::<T>::NAME,
                params
            ))
        }
        let k1 = params.remove(0);
        let k2 = params.remove(0).to_operator();
        let k3 = params.remove(0).to_operator();
        match (k1, k2, k3) {
            (Parameter::Constant(c), Some(sx), Some(sy)) => Correlation::new(c as usize, sx, sy),
            _ => throw!(anyhow!(
                "{} expect a constant and two series",
                Correlation::<T>::NAME,
            )),
        }
    }
}
