<<<<<<< HEAD
pub(crate) mod image;
use crate::util::error::GyResult;
=======
pub mod error;
pub mod image;
use crate::error::GyResult;
>>>>>>> 552a6b1108881e635c37b79112b4a44f67fe8bdc
use std::io::{BufRead, BufReader, BufWriter, Seek};

pub trait ImageEmbed {
    fn embed<R: BufRead + Seek>(r: R, image_format: &str) -> GyResult<()>;
}
