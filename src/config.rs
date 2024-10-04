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
    wal_fname: PathBuf,
    fsize: usize,
}

impl Default for ConfigBuilder {
    fn default() -> ConfigBuilder {
        ConfigBuilder {
            collect_name: "my_index".to_string(),
            io_type: IOType::MMAP,
            data_path: PathBuf::from("./"),
            wal_fname: PathBuf::from(WAL_FILE),
            fsize: DEFAULT_WAL_FILE_SIZE,
        }
    }
}

impl ConfigBuilder {
    pub fn collect_name(mut self, collect_name: String) -> ConfigBuilder {
        self.collect_name = collect_name;
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
        let wal_path = self
            .data_path
            .join(&self.collect_name)
            .join(&self.wal_fname);
        Config {
            collect_name: self.collect_name,
            data_path: self.data_path,
            io_type: self.io_type,
            wal_fname: self.wal_fname,
            fsize: self.fsize,
        }
    }
}

pub struct Config {
    collect_name: String,
    data_path: PathBuf,
    io_type: IOType,
    wal_fname: PathBuf,
    fsize: usize,
}

impl Config {
    pub fn get_collect_name(&self) -> &str {
        &self.collect_name
    }

    pub fn get_data_path(&self) -> &Path {
        &self.data_path
    }

    pub fn get_collection_path(&self) -> PathBuf {
        self.data_path.join(&self.collect_name)
    }

    // pub fn get_wal_path(&self) -> PathBuf {
    //     self.get_data_path()
    //         .join(self.get_collect_name())
    //         .join(self.get_wal_fname())
    // }

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
    parent: Vec<String>,
    level: i32,
}

impl DiskFileMeta {
    pub fn tensor_entry(&self) -> &TensorEntry {
        self.meta.tensor_entry()
    }

    pub fn get_fields(&self) -> &[FieldEntry] {
        self.meta.get_fields()
    }
}
