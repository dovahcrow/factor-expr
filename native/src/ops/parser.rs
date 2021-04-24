use super::*;
use crate::ticker_batch::TickerBatch;
use anyhow::{anyhow, Error};
use fehler::{throw, throws};
use lexpr::{self, Cons, Value};
use std::iter::FromIterator;

pub enum Parameter<T: TickerBatch> {
    Constant(f64),
    Symbol(String),
    Operator(BoxOp<T>),
}

impl<T: TickerBatch> std::fmt::Display for Parameter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Parameter::Constant(v) => write!(f, "{}", v),
            Parameter::Symbol(v) => write!(f, "{}", v),
            Parameter::Operator(v) => write!(f, "{}", v.to_string()),
        }
    }
}

impl<T: TickerBatch> std::fmt::Debug for Parameter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl<T: TickerBatch> Parameter<T> {
    pub fn to_operator(self) -> Option<BoxOp<T>> {
        match self {
            Parameter::Operator(op) => Some(op),
            Parameter::Symbol(_) => None,
            Parameter::Constant(c) => Some(c.boxed()),
        }
    }
}

#[throws(Error)]
pub fn from_str<T: TickerBatch>(repr: &str) -> BoxOp<T> {
    let expr = lexpr::from_str(repr)?;
    let expr = match expr {
        Value::Bool(b) => throw!(anyhow!("unexpected bool {}", b)),
        Value::Bytes(b) => throw!(anyhow!("unexpected bytes {:?}", b)),
        Value::Char(c) => throw!(anyhow!("unexpected char {}", c)),
        Value::Cons(cons) => cons,
        Value::Keyword(k) => throw!(anyhow!("unexpected keyword {}", k)),
        Value::String(s) => throw!(anyhow!("unexpected string {}", s)),
        Value::Symbol(s) => {
            if s.starts_with(":") {
                return Getter::new(&s[1..]).boxed();
            } else {
                throw!(anyhow!("unexpected symbol {}", s))
            }
        }
        Value::Vector(v) => throw!(anyhow!("unexpected vector {:?}", v)),
        _ => throw!(anyhow!("unexpected value")),
    };

    visit(expr)?
}

#[throws(Error)]
fn visit<T: TickerBatch>(expr: Cons) -> BoxOp<T> {
    let expr = expr.to_vec().0;
    let (func, params) = match &*expr {
        [func, params @ ..] => (func, params),
        _ => unimplemented!(),
    };

    let func = match func {
        Value::Symbol(func) => &**func,
        _ => throw!(anyhow!("function name should be symbol")),
    };

    let params = params
        .into_iter()
        .map(|p| match p {
            Value::Number(c) => Ok(Parameter::Constant(c.as_f64().unwrap())),
            Value::Cons(expr) => Ok(Parameter::Operator(visit(expr.clone())?)),
            Value::Symbol(sym) => {
                if sym.starts_with(":") {
                    Ok(Parameter::Operator(Box::new(Getter::new(&sym[1..]))))
                } else {
                    Ok(Parameter::Symbol(sym.to_string()))
                }
            }
            _ => unimplemented!(),
        })
        .collect::<Result<Vec<_>>>()?;

    match func {
        // arithmetics
        Add::<T>::NAME => Result::<Add<T>>::from_iter(params)?.boxed(),
        Sub::<T>::NAME => Result::<Sub<T>>::from_iter(params)?.boxed(),
        Mul::<T>::NAME => Result::<Mul<T>>::from_iter(params)?.boxed(),
        Div::<T>::NAME => Result::<Div<T>>::from_iter(params)?.boxed(),
        Pow::<T>::NAME => Result::<Pow<T>>::from_iter(params)?.boxed(),
        Neg::<T>::NAME => Result::<Neg<T>>::from_iter(params)?.boxed(),
        SignedPow::<T>::NAME => Result::<SignedPow<T>>::from_iter(params)?.boxed(),
        LogAbs::<T>::NAME => Result::<LogAbs<T>>::from_iter(params)?.boxed(),
        Sign::<T>::NAME => Result::<Sign<T>>::from_iter(params)?.boxed(),
        Abs::<T>::NAME => Result::<Abs<T>>::from_iter(params)?.boxed(),

        // logics
        If::<T>::NAME => Result::<If<T>>::from_iter(params)?.boxed(),
        And::<T>::NAME => Result::<And<T>>::from_iter(params)?.boxed(),
        Or::<T>::NAME => Result::<Or<T>>::from_iter(params)?.boxed(),
        Lt::<T>::NAME => Result::<Lt<T>>::from_iter(params)?.boxed(),
        Lte::<T>::NAME => Result::<Lte<T>>::from_iter(params)?.boxed(),
        Gt::<T>::NAME => Result::<Gt<T>>::from_iter(params)?.boxed(),
        Gte::<T>::NAME => Result::<Gte<T>>::from_iter(params)?.boxed(),
        Eq::<T>::NAME => Result::<Eq<T>>::from_iter(params)?.boxed(),
        Not::<T>::NAME => Result::<Not<T>>::from_iter(params)?.boxed(),

        // windows
        TSSum::<T>::NAME => Result::<TSSum<T>>::from_iter(params)?.boxed(),
        TSMean::<T>::NAME => Result::<TSMean<T>>::from_iter(params)?.boxed(),
        TSCorrelation::<T>::NAME => Result::<TSCorrelation<T>>::from_iter(params)?.boxed(),
        TSMin::<T>::NAME => Result::<TSMin<T>>::from_iter(params)?.boxed(),
        TSMax::<T>::NAME => Result::<TSMax<T>>::from_iter(params)?.boxed(),
        TSArgMin::<T>::NAME => Result::<TSArgMin<T>>::from_iter(params)?.boxed(),
        TSArgMax::<T>::NAME => Result::<TSArgMax<T>>::from_iter(params)?.boxed(),
        TSStdev::<T>::NAME => Result::<TSStdev<T>>::from_iter(params)?.boxed(),
        TSSkew::<T>::NAME => Result::<TSSkew<T>>::from_iter(params)?.boxed(),
        Delay::<T>::NAME => Result::<Delay<T>>::from_iter(params)?.boxed(),
        TSRank::<T>::NAME => Result::<TSRank<T>>::from_iter(params)?.boxed(),
        TSLogReturn::<T>::NAME => Result::<TSLogReturn<T>>::from_iter(params)?.boxed(),
        _ => throw!(anyhow!("Unknown function '{}'", func)),
    }
}

#[cfg(test)]
mod test {
    use arrow::record_batch::RecordBatch;

    #[test]
    fn t1() {
        let repr = "(+ :bid_price :ask_price)";
        let op = super::from_str::<RecordBatch>(repr).unwrap();

        let s = op.to_string();
        assert_eq!(s, repr);
    }
}
