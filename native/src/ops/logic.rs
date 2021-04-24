use super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::cmp::max;
use std::iter::FromIterator;
use std::mem;

// #[derive(Clone)]
pub struct If<T> {
    cond: BoxOp<T>,
    btrue: BoxOp<T>,
    bfalse: BoxOp<T>,
    warmup: usize,
}

impl<T> Clone for If<T> {
    fn clone(&self) -> Self {
        Self::new(self.cond.clone(), self.btrue.clone(), self.bfalse.clone())
    }
}

impl<T> If<T> {
    pub fn new(cond: BoxOp<T>, btrue: BoxOp<T>, bfalse: BoxOp<T>) -> Self {
        Self {
            cond,
            btrue,
            bfalse,
            warmup: 0,
        }
    }
}

impl<T> Named for If<T> {
    const NAME: &'static str = "If";
}

impl<T: TickerBatch> Operator<T> for If<T> {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        let conds = &*&*self.cond.update(tb)?;
        let btrues = &*&*self.btrue.update(tb)?;
        let bfalses = &*&*self.bfalse.update(tb)?;

        let mut results = Vec::with_capacity(conds.len());

        let mut i = 0;
        while i + self.warmup < self.ready_offset() && i < conds.len() {
            results.push(f64::NAN);
            i += 1;
        }
        self.warmup += i;

        // No need to do fchecked since we just forward the value
        for i in i..conds.len() {
            if conds[i] > 0. {
                results.push(btrues[i]);
            } else {
                results.push(bfalses[i]);
            }
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        let l = max(self.cond.ready_offset(), self.btrue.ready_offset());
        max(l, self.bfalse.ready_offset())
    }

    fn to_string(&self) -> String {
        format!(
            "({} {} {} {})",
            Self::NAME,
            self.cond.to_string(),
            self.btrue.to_string(),
            self.bfalse.to_string()
        )
    }

    fn depth(&self) -> usize {
        1 + max(
            max(self.cond.depth(), self.btrue.depth()),
            self.bfalse.depth(),
        )
    }

    fn len(&self) -> usize {
        self.cond.len() + self.btrue.len() + self.bfalse.len() + 1
    }

    fn child_indices(&self) -> Vec<usize> {
        let ncond = self.cond.len();
        let nbtrue = self.btrue.len();

        vec![1, ncond + 1, ncond + nbtrue + 1]
    }

    fn columns(&self) -> Vec<String> {
        self.cond
            .columns()
            .into_iter()
            .chain(self.btrue.columns())
            .chain(self.bfalse.columns())
            .collect()
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i == 0 {
            return self.clone().boxed();
        }

        let ncond = self.cond.len();
        let nbtrue = self.btrue.len();
        let nbfalse = self.bfalse.len();

        let i = i - 1;

        if i < ncond {
            self.cond.get(i)?
        } else if i >= ncond && i < ncond + nbtrue {
            self.btrue.get(i - ncond)?
        } else if i >= ncond + nbtrue && i < ncond + nbtrue + nbfalse {
            self.bfalse.get(i - ncond - nbtrue)?
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

        let ncond = self.cond.len();
        let nbtrue = self.btrue.len();
        let nbfalse = self.bfalse.len();

        if i < ncond {
            if i == 0 {
                return mem::replace(&mut self.cond, op) as BoxOp<T>;
            }
            self.cond.insert(i, op)?
        } else if i >= ncond && i < ncond + nbtrue {
            if i - ncond == 0 {
                return mem::replace(&mut self.btrue, op) as BoxOp<T>;
            }
            self.btrue.insert(i - ncond, op)?
        } else if i >= ncond + nbtrue && i < ncond + nbtrue + nbfalse {
            if i - ncond - nbtrue == 0 {
                return mem::replace(&mut self.bfalse, op) as BoxOp<T>;
            }
            self.bfalse.insert(i - ncond - nbtrue, op)?
        } else {
            throw!()
        }
    }
}

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<If<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> If<T> {
        let mut iter = iter.into_iter();

        let cond = iter
            .next()
            .unwrap()
            .to_operator()
            .ok_or_else(|| anyhow!("<cond> for If should be an operator"))?;
        let btrue = iter
            .next()
            .unwrap()
            .to_operator()
            .ok_or_else(|| anyhow!("<btrue> for If should be an operator"))?;
        let bfalse = iter
            .next()
            .unwrap()
            .to_operator()
            .ok_or_else(|| anyhow!("<bfalse> for If should be an operator"))?;

        if iter.count() != 0 {
            throw!(anyhow!("Too many parameters for If"))
        }

        If::new(cond, btrue, bfalse)
    }
}

macro_rules! impl_logic_bivariate {
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

            impl<T: TickerBatch> Operator<T> for $op<T>
            {
                #[throws(Error)]
                fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
                    let ls = &*self.l.update(tb)?;
                    let rs = &*self.r.update(tb)?;

                    let mut results = Vec::with_capacity(ls.len());

                    let mut i = 0;
                    while i + self.warmup < self.ready_offset() && i < ls.len() {
                        results.push(f64::NAN);
                        i += 1;
                    }
                    self.warmup += i;

                    for i in i..ls.len() {
                        let val = ($($func)+) (ls[i], rs[i]) as u64 as f64;
                        results.push(self.fchecked(val)?);
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

                fn child_indices(&self) -> Vec<usize> {
                    vec![1, self.l.len() + 1]
                }

                fn columns(&self) -> Vec<String> {
                    self.l
                        .columns()
                        .into_iter()
                        .chain(self.r.columns())
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
                            return mem::replace(&mut self.l, op) as BoxOp<T>;
                        }
                        self.l.insert(i, op)?
                    } else if i >= nl && i < nl + nr {
                        if i - nl == 0 {
                            return mem::replace(&mut self.r, op) as BoxOp<T>;
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

impl_logic_bivariate! (
    [< => Lt: |l: f64, r: f64| l < r]
    [<= => Lte: |l: f64, r: f64| l <= r]
    [> => Gt: |l: f64, r: f64| l > r]
    [>= => Gte: |l: f64, r: f64| l >= r]
    [== => Eq: |l: f64, r: f64| l == r]
    [And => And: |l: f64, r: f64| l > 0. && r > 0.]
    [Or => Or: |l: f64, r: f64| l > 0. || r > 0.]
);

pub struct Not<T> {
    s: BoxOp<T>,
    warmup: usize,
}

impl<T> Clone for Not<T> {
    fn clone(&self) -> Self {
        Self::new(self.s.clone())
    }
}

impl<T> Not<T> {
    pub fn new(s: BoxOp<T>) -> Self {
        Self { s, warmup: 0 }
    }
}

impl<T> Named for Not<T> {
    const NAME: &'static str = "!";
}

impl<T: TickerBatch> Operator<T> for Not<T> {
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

        // No need to do fchecked since we just forward the value
        for i in i..ss.len() {
            let val = if ss[i] > 0. { 0. } else { 1. };
            results.push(val);
        }

        results.into()
    }

    fn ready_offset(&self) -> usize {
        0
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

impl<T: TickerBatch> FromIterator<Parameter<T>> for Result<Not<T>> {
    #[throws(Error)]
    fn from_iter<A: IntoIterator<Item = Parameter<T>>>(iter: A) -> Not<T> {
        let mut params: Vec<_> = iter.into_iter().collect();
        if params.len() != 1 {
            throw!(anyhow!("Not expect one series, got {:?}", params))
        }
        let k1 = params.remove(0);
        Not::new(
            k1.to_operator()
                .ok_or_else(|| anyhow!("<param> for Not should be an operator"))?,
        )
    }
}
