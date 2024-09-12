use galois::Shape;
use galois::Tensor;
use memmap2::Mmap;
use rand::Rng;
use searchlite::ann::AnnIndex;
use searchlite::ann::HNSW;
use searchlite::disk::MmapReader;
use searchlite::schema::VectorSerialize;
use searchlite::schema::*;
use searchlite::util::fs::GyFile;
use std::fs::File;
use std::io::Write;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
fn main() {
    let mut hnsw = HNSW::<Tensor>::new(32);
    let array_count = 10000;
    let array_length = 133;

    // 创建随机数生成器
    let mut rng = rand::thread_rng();
    let mut arrays: Vec<[f32; 133]> = Vec::with_capacity(array_count);
    // 填充数组
    for _ in 0..array_count {
        let mut arr = [0.0_f32; 133]; // 初始化长度为 100 的 f32 数组
        for i in 0..array_length {
            arr[i] = rng.gen::<f32>(); // 填充随机数
        }
        arrays.push(arr);
    }
    let time1 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    for v in &arrays {
        hnsw.insert(Tensor::from_slice(v, 1, Shape::from_array([133])))
            .unwrap();
    }
    let time2 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();

    println!("time:{}", time2 - time1);

    // let mut arr = [0.0_f32; 133]; // 初始化长度为 100 的 f32 数组
    // for i in 0..array_length {
    //     arr[i] = rng.gen::<f32>(); // 填充随机数
    // }

    // let neighbors = hnsw.query(&Tensor::arr_array(arr), 4);

    let mut file = File::create("./data.hnsw").unwrap();
    hnsw.vector_serialize(&mut file).unwrap();
    file.flush().unwrap();

    let mut file = GyFile::open("./data.hnsw").unwrap();
    let file_size = file.fsize().unwrap();
    let mmap: Mmap = unsafe { memmap2::MmapOptions::new().map(file.file()).unwrap() };
    let mut mmap_reader = MmapReader::new(&mmap, 0, file_size);
    let entry = TensorEntry::new(1, [133], VectorType::F32);
    let hnsw = HNSW::<Tensor>::vector_deserialize(&mut mmap_reader, &entry).unwrap();
    // hnsw.print();
    for (i, v) in hnsw.get_vectors().iter().enumerate() {
        let s = unsafe { v.as_slice::<f32>() };
        assert!(arrays[i] == s)
    }
    println!("{:?}", "finish");
}
