use super::super::{parser::Parameter, BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use fehler::{throw, throws};
use std::{borrow::Cow, collections::VecDeque, iter::FromIterator, mem};

macro_rules! impl_minmax {
    ($($op:ident $cmp:tt {$($vfunc:tt)+})+) => {
        $(
            pub struct $op<T> {
                win_size: usize,
                inner: BoxOp<T>,

                window: VecDeque<(usize, f64)>,
                seq: usize,
                i: usize,
            }

            impl<T> Clone for $op<T> {
                fn clone(&self) -> Self {
                    Self::new(self.win_size, self.inner.clone())
                }
            }

            impl<T> $op<T> {
                pub fn new(win_size: usize, inner: BoxOp<T>) -> Self {
                    Self {
                        win_size,
                        inner,

                        window: VecDeque::new(),
                        seq: 0,
                        i: 0,
                    }
                }
            }

            impl<T> Named for $op<T> {
                const NAME: &'static str = stringify!($op);
            }

            impl<T: TickerBatch> Operator<T> for $op<T> {
                fn reset(&mut self) {
                    self.inner.reset();
                    self.window.clear();
                    self.seq = 0;
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

                        self.seq += 1;

                        while let Some((seq_old, _)) = self.window.front() {
                            if seq_old + self.win_size <= self.seq {
                                self.window.pop_front();
                            } else {
                                break;
                            }
                        }

                        while let Some((_, last_val)) = self.window.back() {
                            if val $cmp *last_val {
                                self.window.pop_back();
                            } else {
                                break;
                            }
                        }

                        self.window.push_back((self.seq, val));

                        let val = if self.i >= self.ready_offset() {
                            let val = ($($vfunc)+) (&self.window, self.seq, self.win_size);
                            val
                        } else {
                            self.i += 1;
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
                    format!("({} {} {})", Self::NAME, self.win_size, self.inner.to_string())
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
                            return mem::replace(&mut self.inner, op)  as BoxOp<T>;
                        }
                        self.inner.insert(i, op)?
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
    Min < { |window: &VecDeque<(usize, f64)>, _: usize, _: usize| window.front().unwrap().1 }
    Max > { |window: &VecDeque<(usize, f64)>, _: usize, _: usize| window.front().unwrap().1 }
    ArgMin < { |window: &VecDeque<(usize, f64)>, seq: usize, win_size: usize| (window.front().unwrap().0 + win_size - seq - 1) as f64 }
    ArgMax > { |window: &VecDeque<(usize, f64)>, seq: usize, win_size: usize| (window.front().unwrap().0 + win_size - seq - 1) as f64 }
}
