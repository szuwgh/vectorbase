#[cfg(test)]
mod tests {
    use embed::image::DefaultImageEmbed;
    use embed::image::ImageSize;
    use embed::image::ModelConfig;
    use std::fs::File;
    use std::io::BufReader;
    #[test]
    fn embed_image() {
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
        let image_path = dir_path.join("../images").join("cat.jpeg");
        let model = DefaultImageEmbed::new(config);
        let f = File::open(image_path).unwrap(); // Read<[u8]>
        let f = BufReader::new(f);
        let res = model.embed(f, "jpeg").unwrap();
        println!("{:?},{}", res, res.len());
    }
}
