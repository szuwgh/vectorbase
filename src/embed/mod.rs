pub(crate) mod image;
use crate::util::error::GyResult;
use std::io::{BufRead, BufReader, BufWriter, Seek};

pub trait ImageEmbed {
    fn embed<R: BufRead + Seek>(r: R, image_format: &str) -> GyResult<()>;
}
