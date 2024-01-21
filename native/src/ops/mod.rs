mod arithmetic;
mod constant;
mod getter;
mod logic;
mod overlap_studies;
mod parser;
mod window;

pub use arithmetic::*;
pub use getter::*;
pub use logic::*;
pub use overlap_studies::*;
pub use parser::from_str;
pub use window::*;

use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error, Result};
use dyn_clone::DynClone;
use fehler::{throw, throws};
use std::borrow::Cow;

pub type BoxOp<T> = Box<dyn Operator<T>>;

pub trait Named {
    const NAME: &'static str;
}

pub trait Operator<T>: Send + DynClone + 'static
where
    T: TickerBatch,
{
    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]>;
    fn ready_offset(&self) -> usize; // A.K.A. at offset the output of factor is first time not nan
    fn to_string(&self) -> String;

    fn len(&self) -> usize;
    fn depth(&self) -> usize;
    fn child_indices(&self) -> Vec<usize>;
    fn columns(&self) -> Vec<String>;
    fn get(&self, i: usize) -> Option<BoxOp<T>>;
    fn insert(&mut self, i: usize, subtree: BoxOp<T>) -> Option<BoxOp<T>>; // insert the subtree, return the subtree swaped out

    fn boxed(self) -> BoxOp<T>
    where
        Self: Sized,
    {
        Box::new(self)
    }

    #[throws(Error)]
    fn fchecked(&self, f: f64) -> f64 {
        let c = f.classify();
        if matches!(c, std::num::FpCategory::Infinite) {
            throw!(anyhow!("{} produced a NaN", self.to_string()))
        } else if matches!(c, std::num::FpCategory::Nan) {
            throw!(anyhow!("{} produced a inf", self.to_string()))
        }
        f
    }
}

impl<T> Clone for BoxOp<T> {
    fn clone(&self) -> BoxOp<T> {
        dyn_clone::clone_box(&**self)
    }
}
