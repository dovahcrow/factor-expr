/// copied from float_ord library
use std::cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd};
use std::{
    fmt::{Debug, Display},
    hash::{Hash, Hasher},
    marker::PhantomData,
};

pub trait SortOrder {
    fn convert(f: f64) -> u64;
}

#[derive(Copy, Clone)]
pub struct Descending;
impl SortOrder for Descending {
    fn convert(f: f64) -> u64 {
        let u: u64 = unsafe { std::mem::transmute(f) };
        let bit = 1 << 63;
        let v = if u & bit == 0 { u | bit } else { !u };
        !v
    }
}

#[derive(Copy, Clone)]
pub struct Ascending;

impl SortOrder for Ascending {
    fn convert(f: f64) -> u64 {
        let u: u64 = unsafe { std::mem::transmute(f) };
        let bit = 1 << 63;
        let v = if u & bit == 0 { u | bit } else { !u };
        v
    }
}

#[derive(Clone, Copy)]
pub struct Float<Ord>(pub f64, u64, PhantomData<Ord>);

impl<O: SortOrder> Float<O> {
    pub fn new(f: f64) -> Float<O> {
        Float(f, O::convert(f), PhantomData)
    }
}

impl<O> PartialEq for Float<O> {
    fn eq(&self, other: &Self) -> bool {
        self.1.eq(&other.1)
    }
}

impl<O> Eq for Float<O> {}
impl<O> PartialOrd for Float<O> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.1.partial_cmp(&other.1)
    }
}
impl<O> Ord for Float<O> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1)
    }
}
impl<O> Hash for Float<O> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.1.hash(state)
    }
}

impl<O> Display for Float<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<O> Debug for Float<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait IntoFloat {
    fn asc(&self) -> Float<Ascending>;
    fn desc(&self) -> Float<Descending>;
}

impl IntoFloat for f64 {
    fn asc(&self) -> Float<Ascending> {
        Float::<Ascending>::new(*self)
    }
    fn desc(&self) -> Float<Descending> {
        Float::<Descending>::new(*self)
    }
}

impl Into<Float<Ascending>> for f64 {
    fn into(self) -> Float<Ascending> {
        self.asc()
    }
}

impl Into<Float<Descending>> for f64 {
    fn into(self) -> Float<Descending> {
        self.desc()
    }
}

#[cfg(test)]
mod tests {
    use super::{Ascending, Descending, Float};
    use rand::{distributions::Uniform, thread_rng, Rng};
    use std::{
        collections::hash_map::DefaultHasher,
        f64::{INFINITY, NAN},
        hash::{Hash, Hasher},
    };

    #[test]
    fn test_ord() {
        assert!(Float::<Ascending>::new(1.0f64) < Float::<Ascending>::new(2.0f64));
        assert!(Float::<Descending>::new(1.0f64) > Float::<Descending>::new(2.0f64));

        assert!(Float::<Ascending>::new(1.0f64) == Float::<Ascending>::new(1.0f64));
        assert!(Float::<Descending>::new(1.0f64) == Float::<Descending>::new(1.0f64));

        assert!(Float::<Ascending>::new(0.0f64) > Float::<Ascending>::new(-0.0f64));
        assert!(Float::<Descending>::new(0.0f64) < Float::<Descending>::new(-0.0f64));

        assert!(Float::<Ascending>::new(NAN) == Float::<Ascending>::new(NAN));
        assert!(Float::<Ascending>::new(-NAN) < Float::<Ascending>::new(NAN));
        assert!(Float::<Ascending>::new(-INFINITY) < Float::<Ascending>::new(INFINITY));
        assert!(Float::<Ascending>::new(INFINITY) < Float::<Ascending>::new(NAN));
        assert!(Float::<Ascending>::new(-NAN) < Float::<Ascending>::new(INFINITY));
    }

    #[test]
    fn test_ord_numbers() {
        let distr = Uniform::new(0., 100000.);
        let rng = thread_rng();
        for n in 0..16 {
            for l in 0..16 {
                let v = rng
                    .clone()
                    .sample_iter(&distr)
                    .map(|x| x % (1 << l) as i64 as f64)
                    .take(1 << n)
                    .collect::<Vec<_>>();
                assert!(v.windows(2).all(|w| (w[0] <= w[1])
                    == (Float::<Ascending>::new(w[0]) <= Float::<Ascending>::new(w[1]))));
            }
        }

        for n in 0..16 {
            for l in 0..16 {
                let v = rng
                    .clone()
                    .sample_iter(&distr)
                    .map(|x| x % (1 << l) as i64 as f64)
                    .take(1 << n)
                    .collect::<Vec<_>>();
                assert!(v.windows(2).all(|w| (w[0] <= w[1])
                    == (Float::<Descending>::new(w[0]) >= Float::<Descending>::new(w[1]))));
            }
        }
    }

    fn hash<F: Hash>(f: F) -> u64 {
        let mut hasher = DefaultHasher::new();
        f.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_ord_hash() {
        assert_ne!(
            hash(Float::<Ascending>::new(0.0f64)),
            hash(Float::<Ascending>::new(-0.0f64))
        );
        assert_ne!(
            hash(Float::<Descending>::new(0.0f64)),
            hash(Float::<Descending>::new(-0.0f64))
        );

        assert_eq!(
            hash(Float::<Ascending>::new(-0.0f64)),
            hash(Float::<Ascending>::new(-0.0f64))
        );
        assert_eq!(
            hash(Float::<Ascending>::new(0.0f64)),
            hash(Float::<Ascending>::new(0.0f64))
        );

        assert_eq!(
            hash(Float::<Descending>::new(-0.0f64)),
            hash(Float::<Descending>::new(-0.0f64))
        );
        assert_eq!(
            hash(Float::<Descending>::new(0.0f64)),
            hash(Float::<Descending>::new(0.0f64))
        );

        assert_ne!(
            hash(Float::<Ascending>::new(NAN)),
            hash(Float::<Ascending>::new(-NAN))
        );
        assert_ne!(
            hash(Float::<Descending>::new(NAN)),
            hash(Float::<Descending>::new(-NAN))
        );

        assert_eq!(
            hash(Float::<Ascending>::new(NAN)),
            hash(Float::<Ascending>::new(NAN))
        );
        assert_eq!(
            hash(Float::<Ascending>::new(-NAN)),
            hash(Float::<Ascending>::new(-NAN))
        );

        assert_eq!(
            hash(Float::<Descending>::new(NAN)),
            hash(Float::<Descending>::new(NAN))
        );
        assert_eq!(
            hash(Float::<Descending>::new(-NAN)),
            hash(Float::<Descending>::new(-NAN))
        );
    }
}
