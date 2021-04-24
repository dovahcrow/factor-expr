use super::{BoxOp, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::Error;
use fehler::{throw, throws};
use std::borrow::Cow;

impl<T: TickerBatch> Operator<T> for f64 {
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        vec![*self; tb.len()].into()
    }

    fn ready_offset(&self) -> usize {
        0
    }

    fn to_string(&self) -> String {
        format!("{}", self)
    }

    fn depth(&self) -> usize {
        1
    }

    fn len(&self) -> usize {
        1
    }

    fn children_indices(&self) -> Vec<usize> {
        vec![]
    }

    fn symbols(&self) -> Vec<String> {
        vec![]
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i > 0 {
            throw!()
        }

        self.clone().boxed()
    }

    #[throws(as Option)]
    fn insert(&mut self, _: usize, _: BoxOp<T>) -> BoxOp<T> {
        unreachable!("insert subtree into root");
    }
}
