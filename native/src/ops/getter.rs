use super::{BoxOp, Named, Operator};
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error};
use fehler::{throw, throws};
use std::borrow::Cow;

#[derive(Clone)]
pub struct Getter {
    name: String,
    idx: Option<usize>,
}

impl Getter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            idx: None,
        }
    }
}

impl Named for Getter {
    const NAME: &'static str = "Getter";
}

impl<T: TickerBatch> Operator<T> for Getter {
    fn reset(&mut self) {}

    #[throws(Error)]
    fn update<'a>(&mut self, tb: &'a T) -> Cow<'a, [f64]> {
        if matches!(self.idx, None) {
            self.idx = Some(
                tb.index_of(&self.name)
                    .ok_or_else(|| anyhow!("No such colume {}", self.name))?,
            );
        }
        let colid = self.idx.unwrap();

        let col = tb
            .values(colid)
            .ok_or_else(|| anyhow!("No such colume {}", self.name))?;

        for &v in col {
            Operator::<T>::fchecked(self, v)?;
        }

        col.into()
    }

    fn ready_offset(&self) -> usize {
        0
    }

    fn to_string(&self) -> String {
        format!(":{}", self.name)
    }

    fn depth(&self) -> usize {
        1
    }

    fn len(&self) -> usize {
        1
    }

    fn child_indices(&self) -> Vec<usize> {
        vec![]
    }

    fn columns(&self) -> Vec<String> {
        vec![self.name.clone()]
    }

    #[throws(as Option)]
    fn get(&self, i: usize) -> BoxOp<T> {
        if i != 0 {
            throw!()
        }
        self.clone().boxed()
    }

    #[throws(as Option)]
    fn insert(&mut self, _: usize, _: BoxOp<T>) -> BoxOp<T> {
        unreachable!("cannot insert root");
    }
}
