use crate::disk::VectorStore;
use crate::disk::{self};
use crate::util::fs::FileManager;
use crate::GyResult;
use std::path::Path;

unsafe impl Sync for Compaction {}
unsafe impl Send for Compaction {}

pub(crate) struct Compaction {
    cur_level: usize,                  // 当前层数，跟踪需要合并的层
    max_files_per_level: [usize; 5],   // 每一层的文件数量上限
    merge_files_per_level: [usize; 5], // 每一层的文件数量上限
}

impl Compaction {
    pub(crate) fn new() -> Compaction {
        Compaction {
            cur_level: 0,
            max_files_per_level: [2, 2, 2, 2, 1], //[16, 8, 4, 2, 1],  //9,5,3,2,1
            merge_files_per_level: [2, 2, 2, 2, 2], // [9, 5, 3, 2, 2], //9,5,3,2,1
        }
    }

    pub(crate) fn need_table_compact(&mut self, list: &Vec<VectorStore>) -> bool {
        let count = list.iter().filter(|f| f.level() == self.cur_level).count();
        let need = count > self.max_files_per_level[self.cur_level];
        if !need {
            println!("no need cur level:{}", self.cur_level);
            self.cur_level = 0;
        }
        need
    }

    /**
     * 返回要压缩的文件列表
     */
    pub(crate) fn plan(&mut self, list: &Vec<VectorStore>) -> Vec<VectorStore> {
        let mut plan_list: Vec<VectorStore> = list
            .iter()
            .filter(|f| f.level() == self.cur_level)
            .cloned()
            .collect::<Vec<_>>();
        // 按文件大小排序，并截取指定数量的文件
        plan_list.sort_by_key(|f| f.file_size());
        plan_list.truncate(self.merge_files_per_level[self.cur_level]);
        plan_list
    }

    pub(crate) fn compact(&mut self, p: &Path, list: &[VectorStore]) -> GyResult<VectorStore> {
        let level = std::cmp::min(self.cur_level + 1, 4);
        let new_fname = FileManager::get_next_table_fname(p)?;
        disk::merge_much(list, &new_fname, level)?;
        let reader = VectorStore::open(new_fname.parent().unwrap())?;
        println!("path cur level:{}", self.cur_level);
        self.cur_level = (self.cur_level + 1) % self.max_files_per_level.len();
        Ok(reader)
    }
}
