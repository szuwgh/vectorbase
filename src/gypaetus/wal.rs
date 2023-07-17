pub(crate) trait Wal {}

pub(crate) struct MmapWal {}

impl MmapWal {
    pub(crate) fn new() -> MmapWal {
        Self {}
    }
}

impl Wal for MmapWal {}
