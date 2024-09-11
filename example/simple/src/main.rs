use galois::Shape;
use galois::Tensor;
use searchlite::ann::AnnType;
use searchlite::query::Term;
use searchlite::schema::*;
use searchlite::Collection;
use searchlite::IndexConfigBuilder;
use std::path::PathBuf;
fn main() {
    let mut schema = Schema::with_vector(VectorEntry::new(
        "vector",
        AnnType::HNSW,
        TensorEntry::new(1, [4], VectorType::F32),
    ));
    schema.add_field(FieldEntry::str("color"));
    let field_id_color = schema.get_field("color").unwrap();
    let config = IndexConfigBuilder::default()
        .data_path(PathBuf::from("./data"))
        .build();
    let mut collect = Collection::new(schema, config).unwrap();
    {
        let mut d5 = Document::new();
        d5.add_text(field_id_color.clone(), "red");
        let v5 = Vector::from_array([0.0f32, 0.0, 1.0, 1.0], d5);
        collect.add(v5).unwrap();

        let mut d6 = Document::new();
        d6.add_text(field_id_color.clone(), "blue");
        let v6 = Vector::from_array([0.0f32, 1.0, 1.0, 0.0], d6);
        collect.add(v6).unwrap();

        let mut d7 = Document::new();
        d7.add_text(field_id_color.clone(), "red");
        let v7 = Vector::from_array([1.0f32, 0.0, 0.0, 1.0], d7);
        collect.add(v7).unwrap();

        let mut d8 = Document::new();
        d8.add_text(field_id_color.clone(), "yellow");
        let d8 = Vector::from_array([1.0f32, 1.0, 0.0, 0.0], d8);
        collect.add(d8).unwrap();
    }

    let reader = collect.reader();
    let p = reader
        .query(
            &Tensor::from_vec(vec![0.0f32, 0.0, 1.0, 1.0], 1, Shape::from_array([4])),
            2,
        )
        .unwrap();

    for n in p.iter() {
        let doc = reader.vector(n.doc_id()).unwrap();
        println!(
            "id:{},vector:{:?},doc:{:?}",
            n.doc_id(),
            doc.vector().to_vec::<f32>(),
            doc.doc(),
        );
    }

    let p = reader
        .search(Term::from_field_text(field_id_color, "red"))
        .unwrap();

    for n in p.iter() {
        let doc = reader.vector(n.doc_id()).unwrap();
        println!("id:{},doc:{:?}", n.doc_id(), doc.doc(),);
    }
}
