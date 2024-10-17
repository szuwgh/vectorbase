use rand::distributions::Alphanumeric;
use rand::Rng;
use std::path::PathBuf;
use vectorbase::ann::AnnType;
use vectorbase::collection::Collection;
use vectorbase::config::ConfigBuilder;
use vectorbase::schema::Document;
use vectorbase::schema::FieldEntry;
use vectorbase::schema::Schema;
use vectorbase::schema::TensorEntry;
use vectorbase::schema::Vector;
use vectorbase::schema::VectorEntry;
use vectorbase::schema::VectorType;
fn main() {
    let mut schema = Schema::with_vector(VectorEntry::new(
        "vector",
        AnnType::HNSW,
        TensorEntry::new(1, [100], VectorType::F32),
    ));
    schema.add_field(FieldEntry::str("body"));
    schema.add_field(FieldEntry::i32("title"));
    let field_id_title = schema.get_field("title").unwrap();
    let config = ConfigBuilder::default()
        .data_path(PathBuf::from("./data"))
        .collect_name("vector1")
        .build();

    let collection = Collection::new(schema, config).unwrap();
    // 创建一个随机数生成器
    let mut rng = rand::thread_rng();

    // 生成一个包含 100 个 f32 随机数的数组
    let random_array1: [f32; 100] = std::array::from_fn(|_| rng.gen());
    let mut d5 = Document::new();
    d5.add_str(
        field_id_title.clone(),
        (0..5).map(|_| rng.sample(Alphanumeric) as char).collect(),
    );
    let v5 = Vector::from_array(random_array1, d5);
    collection.add(v5).unwrap();

    for _ in 0..5 {
        let mut d6: Document = Document::new();
        d6.add_str(
            field_id_title.clone(),
            (0..5).map(|_| rng.sample(Alphanumeric) as char).collect(),
        );
        let random_array2: [f32; 100] = std::array::from_fn(|_| rng.gen());
        let v6 = Vector::from_array(random_array2, d6);
        collection.add(v6).unwrap();
    }

    std::thread::sleep(std::time::Duration::from_secs(10));
}
