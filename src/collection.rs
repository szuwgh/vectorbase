use crate::disk::DiskStoreReader;
use crate::fs::FileManager;
use crate::schema::DocID;
use crate::schema::ValueSized;
use crate::Meta;
use crate::Schema;
use crate::Vector;
use crate::RUNTIME;
use crate::{Config, Engine};
use crate::{GyError, GyResult};
use galois::Tensor;
use lock_api::RawMutex;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::oneshot;

enum Command {
    MemComp,
    TableComp,
}

pub struct Collection(Arc<CollectionImpl>);

impl Collection {
    fn new(schema: Schema, config: Config) -> GyResult<Collection> {
        let (tx, rx) = oneshot::channel::<Command>();
        let collection_impl = Arc::new(CollectionImpl::new(schema, config, tx)?);
        Self::go_mem_compaction(collection_impl.clone(), rx);
        Ok(Collection(collection_impl))
    }

    fn go_mem_compaction(collection_impl: Arc<CollectionImpl>, rx: oneshot::Receiver<Command>) {
        RUNTIME.spawn(async move {
            collection_impl.mem_compaction(rx).await;
        });
    }
}

unsafe impl Sync for CollectionImpl {}
unsafe impl Send for CollectionImpl {}

pub struct CollectionImpl {
    meta: Meta,
    config: Config,
    mem: Engine,
    imm: Option<Engine>,
    disk_reader: RwLock<Option<Vec<DiskStoreReader>>>,

    mcomp_cmd_tx: oneshot::Sender<Command>,
    // tcomp_cmd_tx: oneshot::Sender<Command>,
    mem_lock: Mutex<()>,
}

impl CollectionImpl {
    fn new(
        schema: Schema,
        config: Config,
        tx: oneshot::Sender<Command>,
    ) -> GyResult<CollectionImpl> {
        let data_path = config.get_data_path();
        if !data_path.is_dir() {
            return Err(GyError::DataPathNotDir(data_path.to_path_buf()));
        }
        let collection_path = config.get_collection_path();
        // 如果这个文件存在 则代表数据库存在
        let (mem, imm, readers) = if collection_path.exists() {
            //获取数据库表中 wal 文件
            let (mem_path, imm_path) = FileManager::get_2wal_file_name(&collection_path, "wal")?;
            let (mem, imm) = match (mem_path, imm_path) {
                (Some(p1), Some(p2)) => (
                    Engine::open(&schema, config.get_engine_config(p1))?,
                    Some(Engine::open(&schema, config.get_engine_config(p2))?),
                ),
                (Some(p1), None) => (Engine::open(&schema, config.get_engine_config(p1))?, None),
                _ => return Err(GyError::ErrCollectionWalInvalid),
            };
            let readers = FileManager::get_table_directories(&collection_path)?
                .iter()
                .map(DiskStoreReader::open)
                .collect::<Result<Vec<_>, _>>()?;

            (mem, imm, readers)
        } else {
            //创建文件夹
            FileManager::mkdir(&collection_path)?;
            let first_wal = FileManager::get_next_wal_name(&collection_path)?;
            let mem = Engine::new(&schema, config.get_engine_config(first_wal))?;
            (mem, None, Vec::new())
        };
        let colletion = Self {
            meta: Meta::new(schema),
            config: config,
            mem: mem,
            imm: imm,
            mcomp_cmd_tx: tx,
            disk_reader: RwLock::new(Some(readers)),
            mem_lock: Mutex::new(()),
        };
        Ok(colletion)
    }

    // async fn t_compaction(&mut self) {
    //     loop {
    //         if let Ok(command) = self.mcomp_cmd_tx.try_recv() {}
    //     }
    // }

    async fn mem_compaction(&self, mut mcomp_cmd_rx: oneshot::Receiver<Command>) {
        loop {
            if let Ok(command) = mcomp_cmd_rx.try_recv() {}
        }
    }

    fn init() {}

    fn make_room_for_write(&self, size: usize) -> GyResult<()> {
        if self.mem.check_room_for_write(size) {
            return Ok(());
        } else {
            // self.mem =
        }
        todo!()
    }

    pub fn add(&self, v: Vector) -> GyResult<DocID> {
        unsafe {
            self.mem_lock.raw().lock();
        }
        self.make_room_for_write(v.bytes_size())?;
        let doc_id = self.mem.add(v)?;
        unsafe {
            self.mem_lock.raw().unlock();
        }
        Ok(doc_id)
    }

    pub fn compact_mem() {}

    pub fn query(&self, v: &Tensor, k: usize) {}
}
