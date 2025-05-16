use crate::config::DATA_FILE;
use crate::{
    compaction::Compaction,
    disk::{self, VectorStore},
    fs::FileManager,
    schema::{FieldID, ValueSized},
    searcher::{BlockReader, Searcher},
    util::{
        asyncio::{self, UnbufferedSender},
        common::IdGenerator,
    },
    Config, Engine, FieldEntry, GyError, GyResult, Meta, Schema, Vector, RUNTIME,
};
use lock_api::RawMutex;
use parking_lot::Mutex;
use std::{borrow::BorrowMut, cell::RefCell, mem, sync::Arc};
use tokio::sync::{mpsc, oneshot, RwLock as TokRwLock};

enum CompAck {
    Done,
    Start,
}

enum Command {
    MemComp(Option<oneshot::Sender<CompAck>>),
    TableComp,
    PauseComp(asyncio::UnbufferedSender<CompAck>),
}

unsafe impl Sync for Collection {}
unsafe impl Send for Collection {}

#[derive(Clone)]
pub struct Collection(Arc<CollectionImpl>);

impl Collection {
    pub fn new(schema: Schema, config: Config) -> GyResult<Collection> {
        let (mcomp_cmd_tx, mcomp_cmd_rx) = mpsc::channel::<Command>(1);
        let (tcomp_cmd_tx, tcomp_cmd_rx) = asyncio::un_buffered_channel::<Command>(); //mpsc::channel::<Command>(1);
        let (mcomp_close_tx, mcomp_close_rx) = mpsc::channel::<()>(1);
        let (tcomp_close_tx, tcomp_close_rx) = mpsc::channel::<()>(1);
        let collection_impl = Arc::new(CollectionImpl::new(
            schema,
            config,
            mcomp_cmd_tx,
            tcomp_cmd_tx,
            mcomp_close_tx,
            tcomp_close_tx,
        )?);
        Self::go_table_compaction(collection_impl.clone(), tcomp_cmd_rx, tcomp_close_rx);
        Self::go_mem_compaction(collection_impl.clone(), mcomp_cmd_rx, mcomp_close_rx);
        Ok(Collection(collection_impl))
    }

    pub fn get_schema(&self) -> &Schema {
        self.0.get_schema()
    }

    pub async fn add(&self, v: Vector) -> GyResult<String> {
        self.0.add(v).await
    }

    pub async fn searcher(&self) -> GyResult<Searcher> {
        self.0.searcher().await
    }

    fn go_table_compaction(
        collection_impl: Arc<CollectionImpl>,
        rx: asyncio::UnbufferedReceiver<Command>,
        close_rx: mpsc::Receiver<()>,
    ) {
        RUNTIME.spawn(async move {
            collection_impl.table_compaction(rx, close_rx).await;
        });
    }

    fn go_mem_compaction(
        collection_impl: Arc<CollectionImpl>,
        rx: mpsc::Receiver<Command>,
        close_rx: mpsc::Receiver<()>,
    ) {
        RUNTIME.spawn(async move {
            collection_impl.mem_compaction(rx, close_rx).await;
        });
    }
}

unsafe impl Sync for CollectionImpl {}
unsafe impl Send for CollectionImpl {}

pub struct CollectionImpl {
    meta: Meta,
    config: Config,
    _id_field_id: FieldID,
    id_gen: RefCell<IdGenerator>,
    mem: TokRwLock<Engine>,
    imm: TokRwLock<Option<Engine>>,
    disk_reader: TokRwLock<Option<Vec<VectorStore>>>,
    mcomp_cmd_tx: mpsc::Sender<Command>, //mpsc::Sender<Command>,
    tcomp_cmd_tx: UnbufferedSender<Command>, //mpsc::Sender<Command>,
    mcomp_close_tx: mpsc::Sender<()>,
    tcomp_close_tx: mpsc::Sender<()>,
    mem_lock: Mutex<()>,
}

impl CollectionImpl {
    fn new(
        mut schema: Schema,
        config: Config,
        mcomp_cmd_tx: mpsc::Sender<Command>,
        tcomp_cmd_tx: asyncio::UnbufferedSender<Command>,
        mcomp_close_tx: mpsc::Sender<()>,
        tcomp_close_tx: mpsc::Sender<()>,
    ) -> GyResult<CollectionImpl> {
        let data_path = config.get_data_path();
        if !data_path.exists() {
            FileManager::mkdir(data_path)?;
        }
        if !data_path.is_dir() {
            return Err(GyError::DataPathNotDir(data_path.to_path_buf()));
        }
        let field_id = schema.add_field(FieldEntry::str("_id"));
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
                .map(|path| VectorStore::open(path))
                .collect::<Result<Vec<_>, _>>()?;
            (mem, imm, readers)
        } else {
            //创建文件夹
            FileManager::mkdir(&collection_path)?;
            let mem_wal = FileManager::get_mem_wal_fname(&collection_path)?;
            let mem = Engine::new(&schema, config.get_engine_config(mem_wal))?;
            (mem, None, Vec::new())
        };
        let colletion = Self {
            meta: Meta::new(schema),
            config: config,
            _id_field_id: field_id,
            id_gen: RefCell::new(IdGenerator::new()),
            mem: TokRwLock::new(mem),
            imm: TokRwLock::new(imm),
            mcomp_cmd_tx: mcomp_cmd_tx,
            tcomp_cmd_tx: tcomp_cmd_tx,
            mcomp_close_tx: mcomp_close_tx,
            tcomp_close_tx: tcomp_close_tx,
            disk_reader: TokRwLock::new(Some(readers)),
            mem_lock: Mutex::new(()),
        };
        Ok(colletion)
    }

    pub fn get_schema(&self) -> &Schema {
        self.meta.get_schema()
    }

    async fn table_compaction(
        &self,
        tcomp_cmd_rx: asyncio::UnbufferedReceiver<Command>,
        mut tcomp_close_rx: mpsc::Receiver<()>,
    ) {
        let mut comp = Compaction::new();
        loop {
            if comp.need_table_compact(self.disk_reader.read().await.as_ref().unwrap()) {
                let cmd_opt = tcomp_cmd_rx.try_recv();
                let close_opt = tcomp_close_rx.try_recv();
                match (cmd_opt, close_opt) {
                    // 收到了关闭信号，退出循环
                    (_, Ok(())) => return,
                    // 收到了命令
                    (Ok(cmd), _) => {
                        match cmd {
                            Command::TableComp => {}
                            Command::PauseComp(chan) => {
                                // 发送压缩开始确认信号
                                let _ = chan.send(CompAck::Start).await;
                            }
                            _ => {}
                        }
                    }
                    // 如果两个通道都没有消息，继续循环
                    (Err(_), Err(_)) => {}
                }
                let (new_list, path_list) = {
                    let disk_reader = self.disk_reader.read().await;
                    let old_list = disk_reader.as_ref().unwrap();
                    // 获取需要压缩的 path_list
                    let path_list = comp.plan(old_list);
                    let new_vector_store = comp
                        .compact(self.config.get_collection_path(), &path_list)
                        .unwrap();
                    //从old_list 过滤掉 path_list
                    let mut new_list: Vec<VectorStore> = old_list
                        .iter()
                        .filter(|&x| {
                            !path_list
                                .iter()
                                .any(|y| x.collection_name() == y.collection_name())
                        })
                        .cloned() // 将引用转换为值
                        .collect();
                    new_list.push(new_vector_store);
                    (new_list, path_list)
                };
                {
                    let old = self.disk_reader.write().await.replace(new_list);
                    if let Some(old_list) = old {
                        for v in old_list.into_iter() {
                            drop(v);
                        }
                    }
                }
                //删除 path_list
                for v in path_list.into_iter() {
                    v.wait().await;
                    let dir_path = v.file_path().parent().unwrap().to_path_buf();
                    drop(v);
                    if dir_path.exists() {
                        // 删除文件
                        std::fs::remove_dir_all(&dir_path);
                    }
                }
                tokio::task::yield_now().await;
                continue;
            }
            //如果不需要压缩 等待压缩信号
            tokio::select! {
               Some(cmd) = tcomp_cmd_rx.recv() => {
                    match cmd {
                        Command::TableComp => { }
                        Command::PauseComp(chan) => {
                            //暂停信号
                            let _ = chan.send(CompAck::Start).await;
                        }
                        _ => {}
                    }
                }
                _ = tcomp_close_rx.recv() => {
                    return;
                }
            }
        }
    }

    async fn mem_compaction(
        &self,
        mut mcomp_cmd_rx: mpsc::Receiver<Command>,
        mut mcomp_close_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
               Some(cmd) = mcomp_cmd_rx.recv() => {
                    match cmd {
                        Command::MemComp(chan) => {
                            let _ = self.do_mem_compaction().await;
                            if let Some(c) = chan {
                                let _ = c.send(CompAck::Done);
                            }
                        }
                        _ => {}
                    }
                }
                _ = mcomp_close_rx.recv() => {
                    return;
                }
            }
        }
    }

    async fn do_mem_compaction(&self) -> GyResult<()> {
        let imm = self.imm.read().await;
        if imm.is_none() {
            return Ok(());
        }
        let (tcomp_pause_tx, mcomp_pause_rx) = asyncio::un_buffered_channel::<CompAck>();
        // 发送table compact 暂停信号
        let _ = self
            .tcomp_cmd_tx
            .send(Command::PauseComp(tcomp_pause_tx))
            .await;
        let c = imm.as_ref().unwrap();
        let table_dir = FileManager::get_next_table_dir(self.config.get_collection_path())?;
        let tmp_table_dir = table_dir.with_extension("tmp");
        FileManager::mkdir(&tmp_table_dir)?;
        let data_fname = tmp_table_dir.join(DATA_FILE);
        disk::persist_collection(c.reader(), &self.meta.schema, &data_fname)?;
        std::fs::rename(tmp_table_dir, &table_dir)?;
        drop(imm);
        let new_disk_reader = VectorStore::open(table_dir);
        match new_disk_reader {
            Ok(reader) => {
                let mut imm_lock = self.imm.write().await;
                let imm_engine = imm_lock.take().unwrap();
                self.disk_reader
                    .write()
                    .await
                    .as_mut()
                    .unwrap()
                    .push(reader);
                drop(imm_lock);
                let _ = mcomp_pause_rx.recv().await;
                //通知进行磁盘文件合并
                let _ = self.tcomp_cmd_tx.try_send(Command::TableComp);
                imm_engine.wait().await;
                drop(imm_engine); //释放 imm engine
            }
            Err(e) => {
                //  println!("{:?}", e);
                let _ = mcomp_pause_rx.recv().await;
            }
        }

        Ok(())
    }

    async fn wait_mem_compaction(&self) -> GyResult<()> {
        let (mcomp_ack_tx, mcomp_ack_rx) = oneshot::channel::<CompAck>();
        self.mcomp_cmd_tx
            .send(Command::MemComp(Some(mcomp_ack_tx)))
            .await?;
        let _ = mcomp_ack_rx.await;
        Ok(())
    }

    async fn make_room_for_write(&self, size: usize) -> GyResult<()> {
        if self.mem.read().await.check_room_for_write(size) {
            return Ok(());
        } else {
            self.wait_mem_compaction().await?;
            let mem_wal = FileManager::get_mem_wal_fname(self.config.get_collection_path())?;
            let imm_wal = FileManager::get_imm_wal_fname(self.config.get_collection_path())?;
            self.mem.read().await.rename_wal(&imm_wal)?;
            let mut imm: Engine =
                Engine::new(&self.meta.schema, self.config.get_engine_config(mem_wal))?;
            let mut old_mem = self.mem.write().await;
            mem::swap(&mut imm, old_mem.borrow_mut());
            self.imm.write().await.replace(imm);
            drop(old_mem);
            let _ = self.mcomp_cmd_tx.send(Command::MemComp(None)).await;
        }
        Ok(())
    }

    pub async fn add(&self, mut v: Vector) -> GyResult<String> {
        unsafe {
            self.mem_lock.raw().lock();
        }
        let x = self.id_gen.borrow_mut().generate_id();
        v.payload.add_str(self._id_field_id, x.clone());
        self.make_room_for_write(v.bytes_size()).await?;
        self.mem.read().await.add(v)?;
        unsafe {
            self.mem_lock.raw().unlock();
        }
        Ok(x)
    }

    pub async fn searcher(&self) -> GyResult<Searcher> {
        let mut block_reader_list: Vec<Box<dyn BlockReader>> =
            vec![Box::new(self.mem.read().await.reader())];
        {
            if let Some(imm) = self.imm.read().await.as_ref() {
                block_reader_list.push(Box::new(imm.reader()));
                if let Some(disk_readers) = self.disk_reader.read().await.as_ref() {
                    block_reader_list.extend(
                        disk_readers
                            .iter()
                            .map(|d| Box::new(d.reader()) as Box<dyn BlockReader>),
                    );
                }
            } else {
                if let Some(disk_readers) = self.disk_reader.read().await.as_ref() {
                    block_reader_list.extend(
                        disk_readers
                            .iter()
                            .map(|d| Box::new(d.reader()) as Box<dyn BlockReader>),
                    );
                }
            }
        }

        Ok(Searcher::new(block_reader_list))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ann::AnnType;
    use crate::config::ConfigBuilder;
    use crate::schema::VectorEntry;
    use crate::schema::VectorType;
    use crate::Document;
    use crate::FieldEntry;
    use crate::PathBuf;
    use crate::TensorEntry;

    #[test]
    fn test_collection() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], VectorType::F32),
        ));

        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();

        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("./data"))
            .collect_name("my_vector")
            .build();

        let collection = Collection::new(schema, config).unwrap();

        //let mut writer1 = index.writer().unwrap();
        {
            //writer1.add(1, &d).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title, "aa");

            let v1 = Vector::from_array([0.0, 0.0, 0.0, 1.0], d1).unwrap();

            collection.add(v1);

            let mut d2 = Document::new();
            d2.add_text(field_id_title, "cc");
            let v2 = Vector::from_array([0.0, 0.0, 1.0, 0.0], d2).unwrap();

            collection.add(v2);

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "aa");

            let v3 = Vector::from_array([0.0, 1.0, 0.0, 0.0], d3).unwrap();

            collection.add(v3);

            let mut d4 = Document::new();
            d4.add_text(field_id_title.clone(), "bb");
            let v4 = Vector::from_array([1.0, 0.0, 0.0, 0.0], d4).unwrap();
            collection.add(v4);

            let mut d5 = Document::new();
            d5.add_text(field_id_title.clone(), "cc");
            let v5 = Vector::from_array([0.0, 0.0, 1.0, 1.0], d5).unwrap();
            collection.add(v5);

            let mut d6 = Document::new();
            d6.add_text(field_id_title.clone(), "aa");
            let v6 = Vector::from_array([0.0, 1.0, 1.0, 0.0], d6).unwrap();
            collection.add(v6);

            let mut d7 = Document::new();
            d7.add_text(field_id_title.clone(), "ff");
            let v7 = Vector::from_array([1.0, 0.0, 0.0, 1.0], d7).unwrap();
            collection.add(v7);
        }
    }

    #[test]
    fn test_tokio() {}
}
