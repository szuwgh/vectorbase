use embed::image::DefaultImageEmbed;
use embed::image::ImageSize;
use embed::image::ModelConfig;
use gypaetus::knn::KnnIndex;
use gypaetus::knn::HNSW;
use std::fs::{self, File};
use std::io::BufReader;

fn main() {
    let dir_path = std::env::current_dir().unwrap();

    println!("{:?}", dir_path);
    let model_path = dir_path.join("../model").join("mobilenetv2-7.onnx");
    let config = ModelConfig {
        model_path: model_path,
        image_size: ImageSize {
            width: 224,
            height: 224,
        },
        layer_name: Some("Reshape_103".to_string()),
    };
    let image_path = dir_path.join("../images/simple_example");
    let check_image_path = dir_path.join("../images").join("7.JPEG");
    let model = DefaultImageEmbed::new(config);
    let f = File::open(check_image_path).unwrap(); // Read<[u8]>
    let f = BufReader::new(f);
    let check_embed = model.embed(f, "jpeg").unwrap();
    let mut hnsw = HNSW::new();
    let mut files: Vec<_> = fs::read_dir(&image_path)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    files.sort_by_key(|dir| dir.file_name());
    for file in files {
        //let file = entry.unwrap();
        let image_path = &image_path.join(file.file_name());
        let f = File::open(image_path).unwrap(); // Read<[u8]>
        let f = BufReader::new(f);
        let embedding = model.embed(f, "jpeg").unwrap();
        hnsw.insert(embedding, 0);
        println!("embeding :{:?}", image_path);
    }
    // hnsw.insert(res, 0);
    let a = hnsw.near(&check_embed);
    println!("{:?}", a)
}
