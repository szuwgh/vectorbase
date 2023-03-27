pub trait C {
    fn create() -> Self;
}

pub trait TB<T>
where
    T: C,
{
    fn bb(&self, t: T);
}

pub struct DD<T>(Box<dyn TB<T>>);

pub struct A<T>
where
    T: C,
{
    b: DD<T>,
}

impl<T> A<T>
where
    T: C,
{
    fn new() -> A<T> {
        Self {
            b: DD(Box::new(B::<T>::new())),
        }
    }
}

pub struct B<T>
where
    T: C,
{
    b: T,
}

impl<U> TB<U> for B<U>
where
    T: C,
{
    fn bb(&self, t: U) {}
}

impl<T> B<T>
where
    T: C,
{
    fn new() -> Self {
        Self { b: T::create() }
    }

    fn into_dd(self) -> DD<T> {
        DD(Box::new(self))
    }
}

fn main() {
    println!("Hello, world!");
}
