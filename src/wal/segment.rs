use crate::fs::MmapSelector;
use crate::util::error::GyResult;
use crate::{fs::IoSelector, wal::options::IOType};
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkType {
    Full = 0,
    First = 1,
    Middle = 2,
    Last = 3,
}

const BLOCK_SIZE: usize = 1 << 15; //32KB

type SegmentID = u32;

pub(super) struct Segment {
    id: SegmentID,
    io_selector: Box<dyn IoSelector>,
}

impl Segment {
    fn open(
        io_type: IOType,
        dir_path: &str,
        ext_name: &str,
        id: SegmentID,
        segment_size: usize,
    ) -> GyResult<Segment> {
        let fname = segment_file_name(dir_path, ext_name, id);
        let io_selector: Box<dyn IoSelector> = match io_type {
            IOType::FILEIO => todo!(),
            IOType::MMAP => Box::new(MmapSelector::new(fname, segment_size)?),
        };
        Ok(Self {
            id: id,
            io_selector: io_selector,
        })
    }

    fn write(data: &[u8]) {}
}

fn segment_file_name(dir_path: &str, ext_name: &str, id: SegmentID) -> PathBuf {
    let file_name = format!("{:09}{}", id, ext_name);
    Path::new(dir_path).join(file_name)
}
