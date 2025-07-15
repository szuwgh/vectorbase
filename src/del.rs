use roaring::RoaringBitmap;

struct DelRecord {
    id: u64,
    time: u64,
}

struct Snapshot {
    id: u64,
    time: u64,
    base: RoaringBitmap,
}

struct DelFile {
    base: RoaringBitmap,
    log: Vec<DelRecord>,
    snapshots: Vec<Snapshot>,
    //  wal: ImmutWal,
}

impl DelFile {}
