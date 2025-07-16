// This is a write-ahead log (WAL) implementation that uses memory-mapped files
use super::options::Options;
use super::segment::Segment;
use crate::GyResult;

struct RwWal {
    active_segment: Segment,
    options: Options,
}

impl RwWal {
    pub(crate) fn new(options: Options) -> GyResult<RwWal> {
        todo!()
    }
}
