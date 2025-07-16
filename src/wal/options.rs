#[derive(Copy, Clone)]
pub enum IOType {
    FILEIO,
    MMAP,
}

pub struct Options {
    pub dir_path: String,
    pub segment_size: i64,
    pub segment_file_ext: String,
    pub sync: bool,
    pub bytes_per_sync: u32,
    pub sync_interval: std::time::Duration,
    pub is_segmented: bool, //是否分段
    pub io_type: IOType,    //IO类型
}
