use crate::disk::{self, VectorStoreReader};

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
            max_files_per_level: [15, 8, 4, 2, 1],  //9,5,3,2,1
            merge_files_per_level: [9, 5, 3, 2, 2], //9,5,3,2,1
        }
    }

    pub(crate) fn need_table_compact(&mut self, list: &Vec<VectorStoreReader>) -> bool {
        let count = list.iter().filter(|f| f.level() == self.cur_level).count();
        count > self.max_files_per_level[self.cur_level]
    }

    pub(crate) fn plan(&mut self, list: &Vec<VectorStoreReader>) -> Vec<VectorStoreReader> {
        let mut plan_list: Vec<VectorStoreReader> = list
            .iter()
            .filter(|f| f.level() == self.cur_level)
            .cloned()
            .collect::<Vec<_>>();
        // 按文件大小排序，并截取指定数量的文件
        plan_list.sort_by_key(|f| f.file_size());
        plan_list.truncate(self.merge_files_per_level[self.cur_level]);
        self.cur_level = (self.cur_level + 1) % self.max_files_per_level.len();
        plan_list
    }

    pub(crate) fn compact(&mut self, list: &Vec<VectorStoreReader>) -> VectorStoreReader {}
}

// pub(crate) struct DiskFileMeta {
//     file_size: usize, // 文件大小
//     doc_num: usize,   // 文档数量
//     level: usize,     // 文件所在层数
// }

// pub(crate) struct Compaction {
//     cur_level: usize, // 当前层数，跟踪需要合并的层
//     max_files_per_level: usize, // 每一层的文件数量上限
// }

// impl Compaction {
//     // 创建一个新的 Compaction 实例
//     pub(crate) fn new() -> Compaction {
//         Self {
//             cur_level: 0,
//             max_files_per_level: 4, // 每层最多 4 个文件
//         }
//     }

//     // 判断是否需要合并：当前层文件数量超过上限时返回 true
//     pub(crate) fn need_compact(&self, list: &Vec<DiskFileMeta>) -> bool {
//         let count = list.iter().filter(|f| f.level == self.cur_level).count();
//         count > self.max_files_per_level
//     }

//     // 执行文件合并，将当前层的文件合并并推入下一层
//     pub(crate) fn compact(&mut self, list: &Vec<DiskFileMeta>) -> Vec<DiskFileMeta> {
//         // 筛选出当前层的文件进行合并
//         let mut cur_level_files: Vec<DiskFileMeta> = list
//             .iter()
//             .filter(|f| f.level == self.cur_level)
//             .cloned()
//             .collect();

//         // 按文件大小排序，优先合并较小的文件
//         cur_level_files.sort_by_key(|f| f.file_size);

//         let mut new_files = Vec::new();
//         let mut temp_file_size = 0;
//         let mut temp_doc_num = 0;

//         // 将当前层的小文件合并为一个大文件，推入下一层
//         for file in cur_level_files {
//             temp_file_size += file.file_size;
//             temp_doc_num += file.doc_num;

//             // 当累计大小达到一定阈值时，创建一个新的文件并放入下一层
//             if temp_file_size >= 10 * 1024 { // 假设每 10KB 触发一次合并
//                 new_files.push(DiskFileMeta {
//                     file_size: temp_file_size,
//                     doc_num: temp_doc_num,
//                     level: self.cur_level + 1,
//                 });
//                 temp_file_size = 0;
//                 temp_doc_num = 0;
//             }
//         }

//         // 如果还有剩余的未合并文件，也创建一个新文件
//         if temp_file_size > 0 {
//             new_files.push(DiskFileMeta {
//                 file_size: temp_file_size,
//                 doc_num: temp_doc_num,
//                 level: self.cur_level + 1,
//             });
//         }

//         // 返回新的文件列表，代表合并后的结果
//         new_files
//     }
// }
