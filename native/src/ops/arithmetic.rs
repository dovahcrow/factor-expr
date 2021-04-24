use super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::cmp::max;
use std::iter::FromIterator;
use std::mem;

macro_rules! impl_arithmetic_bivariate {
    ($([$name:tt => $op:ident: $($func:tt)+])+) => {
        $(
            pub struct $op<T> {
                l: BoxOp<T>,
                r: BoxOp<T>,
                warmup: usize,
            }

            impl<T> Clone for $op<T> {
                fn clone(&self) -> Self {
                    Self::new(self.l.clone(), self.r.clone())
                }
            }

            impl<T> $op<T> {
                pub fn new(l: BoxOp<T>, r: BoxOp<T>) -> Self {
                    Self { l, r, warmup: 0 }
                }
            }

            impl<T> Named for $op<T> {
                const NAME: &'static str = stringify!($name);
            }

            impl<T: TickerBatch> Operator<T> for $op<T> {
                #[throws(Error)]
                fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
                    let (l, r) = (&mut self.l, &mut self.r);
                    let (ls, rs) = rayon::join(|| l.update(tb), || r.update(tb));
                    let (ls, rs) = (ls?, rs?);

                    let mut results = Vec::with_capacity(ls.len());

                    let mut i = 0;
                    while i + self.warmup < self.ready_offset() && i < ls.len() {
                        results.push(f64::NAN);
                        i += 1;
                    }
                    self.warmup += i;

                    for i in i..ls.len() {
                        let val = self.fchecked(($($func)+) (ls[i], rs[i]))?;
                        results.push(val);
                    }

                    results.into()
                }

                fn ready_offset(&self) -> usize {
                    max(self.l.ready_offset(), self.r.ready_offset())
                }

                fn to_string(&self) -> String {
                    format!("({} {} {})", Self::NAME, self.l.to_string(), self.r.to_string())
                }

                fn depth(&self) -> usize {
                    1 + max(self.l.depth(), self.r.depth())
                }

                fn len(&self) -> usize {
                    self.l.len() + self.r.len() + 1
                }

                fn subindices(&self) -> Vec<usize> {
                    vec![1, self.l.len() + 1]
                }

                fn symbols(&self) -> Vec<String> {
                    self.l
                        .symbols()
                        .into_iter()
                        .chain(self.r.symbols())
                        .collect()
                }

                #[throws(as Option)]
                fn get(&self, i: usize) -> BoxOp<T> {
                    if i == 0 {
                        return self.clone().boxed();
                    }
                    let i = i - 1;

                    let nl = self.l.len();
                    let nr = self.r.len();

                    if i < nl {
                        self.l.get(i)?
                    } else if i >= nl && i < nl + nr {
                        self.r.get(i - nl)?
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

                    let nl = self.l.len();
                    let nr = self.r.len();

                    if i < nl {
                        if i == 0 {
                            return mem::replace(&mut self.l, op);
                        }
                        self.l.insert(i, op)?
                    } else if i >= nl && i < nl + nr {
                        if i - nl == 0 {
                            return mem::replace(&mut self.r, op);
                        }
                        self.r.insert(i - nl, op)?
                    } else {
                        throw!()
                    }
                }
            }

            impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<$op<T>> {
                #[throws(Error)]
                fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> $op<T> {
                    let mut params: Vec<_> = iter.into_iter().collect();
                    if params.len() != 2 {
                        throw!(anyhow!(
                            "{} expect two series, got {:?}",
                            stringify!($op), params
                        ))
                    }
                    let k1 = params.remove(0).to_operator().ok_or_else(|| anyhow!(
                        "<param1> for {} should be an operator or constant",
                        stringify!($op)
                    ))?;
                    let k2 = params.remove(0).to_operator().ok_or_else(|| anyhow!(
                        "<param2> for {} should be an operator or constant",
                        stringify!($op)
                    ))?;
                    $op::new(k1, k2)
                }
            }
        )+
    };
}

impl_arithmetic_bivariate! (
    [+ => Add: |l: f64, r: f64| l + r]
    [- => Sub: |l: f64, r: f64| l - r]
    [* => Mul: |l: f64, r: f64| l * r]
    [/ => Div: |l: f64, r: f64| r.signum() * l / if r == 0. { f64::EPSILON } else { r }]
);

macro_rules! impl_arithmetic_univariate {
    ($([$name:tt => $op:ident: $($func:tt)+])+) => {
        $(
            pub struct $op<T> {
                s: BoxOp<T>,
                warmup: usize,
            }

            impl<T> Clone for $op<T> {
                fn clone(&self) -> Self {
                    Self::new(self.s.clone())
                }
            }

            impl<T> $op<T> {
                pub fn new(s: BoxOp<T>) -> Self {
                    Self { s, warmup: 0 }
                }
            }

            impl<T> Named for $op<T> {
                const NAME: &'static str = stringify!($name);
            }

            impl<T: TickerBatch> Operator<T> for $op<T> {
                #[throws(Error)]
                fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
                    let ss = &*self.s.update(tb)?;

                    let mut results = Vec::with_capacity(ss.len());

                    let mut i = 0;
                    while i + self.warmup < self.ready_offset() && i < ss.len() {
                        results.push(f64::NAN);
                        i += 1;
                    }
                    self.warmup += i;

                    for i in i..ss.len() {
                        let val = self.fchecked(($($func)+) (ss[i]))?;
                        results.push(val);
                    }

                    results.into()
                }

                fn ready_offset(&self) -> usize {
                    self.s.ready_offset()
                }

                fn to_string(&self) -> String {
                    format!("({} {})", Self::NAME, self.s.to_string())
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
                            return mem::replace(&mut self.s, op)  as BoxOp<T>;
                        }
                        self.s.insert(i, op)?
                    } else {
                        throw!()
                    }
                }
            }

            impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<$op<T>> {
                #[throws(Error)]
                fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> $op<T> {
                    let mut params: Vec<_> = iter.into_iter().collect();
                    if params.len() != 1 {
                        throw!(anyhow!(
                            "{} expect one series, got {:?}",
                            stringify!($op), params
                        ))
                    }
                    let k1 = params.remove(0).to_operator().ok_or_else(|| anyhow!("<param> for {} should be an operator", stringify!($op)))?;
                    $op::new(k1)
                }
            }
        )+
    };
}

impl_arithmetic_univariate! (
    [LogAbs => LogAbs: |s: f64| (s.abs() + f64::EPSILON).ln()]
    [Sign => Sign: |s: f64| s.signum()]
    [Abs => Abs: |s: f64| s.abs()]
    [Neg => Neg: |s: f64| -s]
);

macro_rules! impl_arithmetic_univariate_1arg {
    ($([$name:tt => $op:ident: $($func:tt)+])+) => {
        $(
            pub struct $op<T> {
                s: BoxOp<T>,
                p: f64,
                warmup: usize,
            }

            impl<T> Clone for $op<T> {
                fn clone(&self) -> Self {
                    Self::new(self.p, self.s.clone())
                }
            }

            impl<T> $op<T> {
                pub fn new(p: f64, s: BoxOp<T>) -> Self {
                    Self { p, s, warmup: 0 }
                }
            }

            impl<T> Named for $op<T> {
                const NAME: &'static str = stringify!($name);
            }

            impl<T: TickerBatch> Operator<T> for $op<T> {
                #[throws(Error)]
                fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
                    let ss = &*self.s.update(tb)?;

                    let mut results = Vec::with_capacity(ss.len());

                    let mut i = 0;
                    while i + self.warmup < self.ready_offset() && i < ss.len() {
                        results.push(f64::NAN);
                        i += 1;
                    }
                    self.warmup += i;

                    for i in i..ss.len() {
                        let val = self.fchecked(($($func)+) (self.p, ss[i]))?;
                        results.push(val);
                    }

                    results.into()
                }

                fn ready_offset(&self) -> usize {
                    self.s.ready_offset()
                }

                fn to_string(&self) -> String {
                    format!("({} {} {})", Self::NAME, self.p, self.s.to_string())
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
                            return mem::replace(&mut self.s, op)  as BoxOp<T>;
                        }
                        self.s.insert(i, op)?
                    } else {
                        throw!()
                    }
                }
            }

            impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<$op<T>> {
                #[throws(Error)]
                fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> $op<T> {
                    let mut params: Vec<_> = iter.into_iter().collect();
                    if params.len() != 2 {
                        throw!(anyhow!(
                            "{} expect one constant and one series, got {:?}",
                            stringify!($op), params
                        ))
                    }

                    let k1 = if let Parameter::Constant(k1) = params.remove(0) {
                        k1
                    } else {
                        throw!(anyhow!("<param> for {} should be a constant", stringify!($op)));
                    };

                    let k2 = params.remove(0).to_operator().ok_or_else(|| anyhow!("<param> for {} should be an operator", stringify!($op)))?;
                    $op::new(k1, k2)
                }
            }
        )+
    };
}

impl_arithmetic_univariate_1arg! {
    [^ => Pow: |p: f64, s: f64| s.powf(p)]
    [SPow => SignedPow: |p: f64, s: f64| s.signum() * s.abs().powf(p)]
}
