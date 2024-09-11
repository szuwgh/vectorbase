#[macro_export]
macro_rules! iocopy {
    ($des:expr, $src:expr) => {
        crate::macros::copy_slice($des, $src)
    };
}

pub(crate) fn copy_slice<T: Copy>(des: &mut [T], src: &[T]) -> usize {
    let l = if des.len() < src.len() {
        des.len()
    } else {
        src.len()
    };
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), des.as_mut_ptr(), l);
    }
    l
}
