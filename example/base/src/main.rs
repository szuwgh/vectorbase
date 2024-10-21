use galois::Tensor;
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
        TensorEntry::new(1, [1000], VectorType::F32),
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
    let size = 100000;
    let mut rng = rand::thread_rng();
    let mut array_list = Vec::with_capacity(size);
    for i in 0..size {
        let random_array2: [f32; 1000] = std::array::from_fn(|_| rng.gen());
        array_list.push(random_array2);
    }
    for i in 0..size {
        let mut d6: Document = Document::new();
        d6.add_str(
            field_id_title.clone(),
            (0..5).map(|_| rng.sample(Alphanumeric) as char).collect(),
        );
        let v6 = Vector::from_array(array_list[i], d6);
        collection.add(v6).unwrap();
        println!("{}", i);
        // std::thread::sleep(std::time::Duration::from_secs(1));
    }
    for i in 0..size {
        let t = Tensor::arr_array(array_list[i]);
        let res = collection.query(&t, 5).unwrap();
        let v = res.first().unwrap().vector().vector().to_vec::<f32>();
        assert!(res.len() == 5);
        //  println!("{:?}，{:?}", &v, array_list[i]);
        assert!(v == array_list[i]);
        // println!("success");
        // std::thread::sleep(std::time::Duration::from_secs(1));
    }

    std::thread::sleep(std::time::Duration::from_secs(10));
}
