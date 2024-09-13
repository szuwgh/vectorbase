use crate::disk::DiskStoreReader;
use galois::Shape;
use galois::Tensor;
use searchlite::ann::AnnType;
use searchlite::disk;
use searchlite::query::Term;
use searchlite::schema::*;
use searchlite::Collection;
use searchlite::IndexConfigBuilder;
use std::path::PathBuf;

fn main() {
    {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();
        let config = IndexConfigBuilder::default()
            .data_path(PathBuf::from("./data"))
            .build();
        let mut collect = Collection::new(schema, config).unwrap();
        //let mut writer1 = index.writer().unwrap();
        {
            //writer1.add(1, &d).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "aa");

            let v1 = Vector::from_array([0.0, 0.0, 0.0, 1.0], d1);

            collect.add(v1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(field_id_title.clone(), "cc");
            let v2 = Vector::from_array([0.0, 0.0, 1.0, 0.0], d2);

            collect.add(v2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "aa");

            let v3 = Vector::from_array([0.0, 1.0, 0.0, 0.0], d3);

            collect.add(v3).unwrap();

            let mut d4 = Document::new();
            d4.add_text(field_id_title.clone(), "bb");
            let v4 = Vector::from_array([1.0, 0.0, 0.0, 0.0], d4);
            collect.add(v4).unwrap();
        }
    }

    {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        //  let p = PathBuf::from("./data_wal/my_index/data.wal");
        let field_id_title = schema.get_field("title").unwrap();

        let config = IndexConfigBuilder::default()
            .data_path(PathBuf::from("./data"))
            .build();
        let collect = Collection::open(schema, config).unwrap();
        let reader = collect.reader();
        let p = reader
            .query(
                &Tensor::from_vec(vec![0.0f32, 0.0, 1.0, 0.0], 1, Shape::from_array([4])),
                4,
            )
            .unwrap();

        for n in p.iter() {
            let doc = reader.vector(n.doc_id()).unwrap();
            println!("docid:{},doc{:?}", n.doc_id(), unsafe {
                doc.vector().to_vec::<f32>()
            });
        }

        let mut d5 = Document::new();
        d5.add_text(field_id_title.clone(), "cc");
        let v5 = Vector::from_array([0.0, 0.0, 1.0, 1.0], d5);
        collect.add(v5).unwrap();

        let mut d6 = Document::new();
        d6.add_text(field_id_title.clone(), "aa");
        let v6 = Vector::from_array([0.0, 1.0, 1.0, 0.0], d6);
        collect.add(v6).unwrap();

        let mut d7 = Document::new();
        d7.add_text(field_id_title.clone(), "ff");
        let v7 = Vector::from_array([1.0, 0.0, 0.0, 1.0], d7);
        collect.add(v7).unwrap();

        let mut d8 = Document::new();
        d8.add_text(field_id_title.clone(), "gg");
        let d8 = Vector::from_array([1.0, 1.0, 0.0, 0.0], d8);
        collect.add(d8).unwrap();

        let reader: searchlite::CollectionReader = collect.reader();
        for (offset, v) in reader.vector_iter() {
            println!(
                "offset:{},doc{:?},vector:{:?}",
                offset,
                v.doc(),
                v.vector().to_vec::<f32>()
            );
        }
    }
}
