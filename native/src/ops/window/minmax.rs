use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::mem;

#[derive(Clone)]
struct Cache {
    history: VecDeque<(usize, f64)>,
    seq: usize,
}

impl Cache {
    fn new() -> Cache {
        Cache {
            history: VecDeque::new(),
            seq: 0,
        }
    }
}

macro_rules! impl_minmax {
    ($($op:ident $cmp:tt {$($vfunc:tt)+})+) => {
        $(
            pub struct $op<T> {
                win_size: usize,
                s: BoxOp<T>,

                cache: Cache,
                warmup: usize,
            }

            impl<T> Clone for $op<T> {
                fn clone(&self) -> Self {
                    Self::new(self.win_size, self.s.clone())
                }
            }

            impl<T> $op<T> {
                pub fn new(win_size: usize, s: BoxOp<T>) -> Self {
                    Self {
                        win_size,
                        s,

                        cache: Cache::new(),
                        warmup: 0,
                    }
                }
            }

            impl<T> Named for $op<T> {
                const NAME: &'static str = stringify!($op);
            }

            impl<T: TickerBatch> Operator<T> for $op<T> {
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
                        let val = ss[i];
                        self.cache.seq += 1;

                        while let Some((seq_old, _)) = self.cache.history.front() {
                            if seq_old + self.win_size <= self.cache.seq {
                                self.cache.history.pop_front();
                            } else {
                                break;
                            }
                        }

                        while let Some((_, last_val)) = self.cache.history.back() {
                            if val $cmp *last_val {
                                self.cache.history.pop_back();
                            } else {
                                break;
                            }
                        }

                        self.cache.history.push_back((self.cache.seq, val));

                        results.push(f64::NAN);
                        i += 1;
                    }
                    self.warmup += i;

                    for i in i..ss.len() {
                        let val = ss[i];
                        self.cache.seq += 1;

                        while let Some((seq_old, _)) = self.cache.history.front() {
                            if seq_old + self.win_size <= self.cache.seq {
                                self.cache.history.pop_front();
                            } else {
                                break;
                            }
                        }

                        while let Some((_, last_val)) = self.cache.history.back() {
                            if val $cmp *last_val {
                                self.cache.history.pop_back();
                            } else {
                                break;
                            }
                        }

                        self.cache.history.push_back((self.cache.seq, val));

                        let result = ($($vfunc)+) (&self.cache, self.win_size);
                        results.push(self.fchecked(result)?);
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

                fn children_indices(&self) -> Vec<usize> {
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
                        throw!(anyhow!("{} expect a constant and a series, got {:?}", $op::<T>::NAME, params))
                    }
                    let k1 = params.remove(0);
                    let k2 = params.remove(0);
                    match (k1, k2) {
                        (Parameter::Constant(c), Parameter::Operator(sub)) => $op::new(c as usize, sub),
                        (a, b) => throw!(anyhow!("{name} expect a constant and a series, got ({name} {} {})", a, b, name = $op::<T>::NAME)),
                    }
                }
            }
        )+
    };
}

impl_minmax! {
    TSMin < { |cache: &Cache, _: usize| cache.history.front().unwrap().1 }
    TSMax > { |cache: &Cache, _: usize| cache.history.front().unwrap().1 }
    TSArgMin < { |cache: &Cache, win_size: usize| (cache.history.front().unwrap().0 + win_size - cache.seq - 1) as f64 }
    TSArgMax > { |cache: &Cache, win_size: usize| (cache.history.front().unwrap().0 + win_size - cache.seq - 1) as f64 }
}
