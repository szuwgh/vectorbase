use crate::schema::DocID;
use crate::schema::Schema;
use crate::FieldEntry;
use crate::IOType;
use crate::Meta;
use crate::PathBuf;
use crate::TensorEntry;
use crate::DEFAULT_WAL_FILE_SIZE;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(crate) const WAL_FILE: &'static str = ".wal"; // 数据
pub(crate) const DATA_FILE: &'static str = "data.gy"; // 数据
pub(crate) const META_FILE: &'static str = "meta.json"; // index 元数据
pub(crate) const DELETE_FILE: &'static str = "ids.del"; // 被删除的id

pub struct ConfigBuilder {
    collect_name: String,
    io_type: IOType,
    data_path: PathBuf,
    fsize: usize,
}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        ConfigBuilder {
            collect_name: "my_vector".to_string(),
            io_type: IOType::MMAP,
            data_path: PathBuf::from("./"),
            fsize: DEFAULT_WAL_FILE_SIZE,
        }
    }
}

impl ConfigBuilder {
    pub fn collect_name(mut self, collect_name: &str) -> ConfigBuilder {
        self.collect_name = collect_name.to_string();
        self
    }

    pub fn io_type(mut self, io_type: IOType) -> ConfigBuilder {
        self.io_type = io_type;
        self
    }

    pub fn data_path(mut self, index_path: PathBuf) -> ConfigBuilder {
        self.data_path = index_path;
        self
    }

    pub fn fsize(mut self, fsize: usize) -> ConfigBuilder {
        self.fsize = fsize;
        self
    }

    pub fn build(self) -> Config {
        let collection_path = self.data_path.join(&self.collect_name);
        Config {
            collect_name: self.collect_name,
            data_path: self.data_path,
            collection_path: collection_path,
            io_type: self.io_type,
            fsize: self.fsize,
        }
    }
}

pub struct Config {
    collect_name: String,
    data_path: PathBuf,
    collection_path: PathBuf,
    io_type: IOType,
    fsize: usize,
}

impl Config {
    pub fn get_collect_name(&self) -> &str {
        &self.collect_name
    }

    pub fn get_data_path(&self) -> &Path {
        &self.data_path
    }

    pub fn get_collection_path(&self) -> &Path {
        &self.collection_path
    }

    pub fn get_meta_path(&self) -> PathBuf {
        self.data_path.join(&self.collect_name).join(META_FILE)
    }

    pub fn get_engine_config(&self, wal_path: PathBuf) -> EngineConfig {
        EngineConfig {
            io_type: self.io_type,
            wal_path: wal_path,
            schema_path: self.data_path.join(META_FILE),
            fsize: self.fsize,
        }
    }
}

#[derive(Clone)]
pub struct EngineConfig {
    io_type: IOType,
    wal_path: PathBuf,
    schema_path: PathBuf,
    fsize: usize,
}

impl EngineConfig {
    pub fn get_io_type(&self) -> &IOType {
        &self.io_type
    }

    pub fn get_wal_path(&self) -> &Path {
        &self.wal_path
    }

    pub fn get_fsize(&self) -> usize {
        self.fsize
    }

    pub fn get_schema_path(&self) -> &Path {
        &self.schema_path
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct DiskFileMeta {
    meta: Meta,
    collection_name: String,
    parent: Vec<String>,
    field_range: Vec<TermRange>,
    file_size: usize, //文件大小
    doc_num: usize,   //文档数量
    level: usize,     //文件所在层数
}

impl DiskFileMeta {
    pub(crate) fn new(
        meta: Meta,
        collection_name: &str,
        parent: Vec<String>,
        field_range: Vec<TermRange>,
        file_size: usize,
        doc_num: usize,
        level: usize,
    ) -> DiskFileMeta {
        DiskFileMeta {
            meta,
            collection_name: collection_name.to_string(),
            parent,
            field_range,
            file_size: file_size,
            doc_num: doc_num,
            level,
        }
    }

    pub fn get_collection_name(&self) -> &str {
        &self.collection_name
    }

    pub fn get_schema(&self) -> &Schema {
        &self.meta.schema
    }

    pub fn vector_name(&self) -> &str {
        self.meta.vector_name()
    }

    pub fn tensor_entry(&self) -> &TensorEntry {
        self.meta.tensor_entry()
    }

    pub fn get_level(&self) -> usize {
        self.level
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }

    pub fn get_fields(&self) -> &[FieldEntry] {
        self.meta.get_fields()
    }

    pub fn get_doc_num(&self) -> usize {
        self.doc_num
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TermRange(Vec<u8>, Vec<u8>);

impl TermRange {
    pub(crate) fn new(start: Vec<u8>, end: Vec<u8>) -> TermRange {
        TermRange(start, end)
    }

    pub(crate) fn start(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn end(&self) -> &[u8] {
        &self.1
    }
}
