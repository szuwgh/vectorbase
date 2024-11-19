use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use galois::Tensor;
use std::path::PathBuf;
use std::thread;
use tokio::runtime::Builder;
use vectorbase::ann::AnnType;
use vectorbase::collection::Collection;
use vectorbase::config::ConfigBuilder;
use vectorbase::query::Term;
use vectorbase::schema::Document;
use vectorbase::schema::FieldEntry;
use vectorbase::schema::FieldID;
use vectorbase::schema::Schema;
use vectorbase::schema::TensorEntry;
use vectorbase::schema::Vector;
use vectorbase::schema::VectorEntry;
use vectorbase::schema::VectorType;

async fn test_query(_id_field_id: FieldID) {
    // With custom InitOptions
    let model = TextEmbedding::try_new(
        InitOptions::new(EmbeddingModel::AllMiniLML6V2)
            .with_show_download_progress(true)
            .with_cache_dir(PathBuf::from("/opt/rsproject/chappie/rust-lib/model")),
    )
    .unwrap();
    let mut schema = Schema::with_vector(VectorEntry::new(
        "vector",
        AnnType::HNSW,
        TensorEntry::new(1, [384], VectorType::F32),
    ));
    schema.add_field(FieldEntry::str("content"));
    let config = ConfigBuilder::default()
        .data_path(PathBuf::from("./data"))
        .collect_name("vector1")
        .build();

    let collection = Collection::new(schema, config).unwrap();
    let test_doc = vec!["She couldn't remember where she put her glasses"];
    let embeddings = model.embed(test_doc, None).unwrap();
    let seacher = collection.searcher().await.unwrap();
    for ds in seacher
        .search(Term::from_field_text(_id_field_id, "AAAAAGc8xKcACmIqJoHo"))
        .unwrap()
    {
        let vc = seacher.vector(&ds).unwrap();
        println!("content:{:?}", vc.doc().get_field_values())
    }
}

async fn test_insert(collection: Collection, field_id_content: FieldID) {
    // With custom InitOptions
    let model = TextEmbedding::try_new(
        InitOptions::new(EmbeddingModel::AllMiniLML6V2)
            .with_show_download_progress(true)
            .with_cache_dir(PathBuf::from("/opt/rsproject/chappie/rust-lib/model")),
    )
    .unwrap();

    let documents = vec![
        "The cat is sleeping under the table.",
        "Artificial intelligence is transforming the world.",
        "She loves hiking in the mountains during summer.",
        "The weather today is cloudy with a chance of rain.",
        "I left my keys on the kitchen counter.",
        "He enjoys reading historical fiction novels.",
        "Can you recommend a good Italian restaurant nearby?",
        "We need to finish this project before the deadline.",
        "The sun rises in the east and sets in the west.",
        "He couldn't remember where he parked the car.",
        "The plane will depart at 5:30 PM from gate 12.",
        "She bought a new pair of running shoes.",
        "Quantum mechanics is a difficult subject to grasp.",
        "The dog barked loudly at the passing car.",
        "He plays the guitar in a local band on weekends.",
        "A watched pot never boils.",
        "They traveled to Paris for their honeymoon.",
        "The concert tickets sold out in minutes.",
        "It is important to drink water throughout the day.",
        "The library closes at 7 PM during weekdays.",
        "I need to book a hotel for my trip to New York.",
        "She won the first prize in the baking competition.",
        "The river flows through the center of the town.",
        "He forgot to turn off the lights before leaving.",
        "The meeting has been postponed until next Monday.",
        "The cake tastes better with a bit of cinnamon.",
        "They adopted a kitten from the animal shelter.",
        "This software update improves performance significantly.",
        "She speaks four languages fluently.",
        "The garden is full of blooming flowers in spring.",
        "You should always wear sunscreen at the beach.",
        "He lost his wallet somewhere in the mall.",
        "The museum has an impressive collection of artifacts.",
        "They arrived at the airport two hours early.",
        "She painted the walls a light shade of blue.",
        "The children are playing soccer in the park.",
        "He didn't expect to win the lottery.",
        "The coffee shop on the corner makes the best espresso.",
        "They invited all their friends to the housewarming party.",
        "He forgot his umbrella and got soaked in the rain.",
        "She takes yoga classes every Wednesday.",
        "The bakery down the street sells fresh bread every morning.",
        "I need to finish reading this book by tomorrow.",
        "The traffic is heavy during rush hour.",
        "He loves photography and travels to take scenic shots.",
        "The movie starts at 8:00 PM sharp.",
        "She ordered a cappuccino and a croissant.",
        "He couldn't solve the last question on the test.",
        "The dog loves chasing squirrels in the yard.",
        "They decorated the house for Christmas with lights and ornaments.",
        "She practices the piano for an hour every day.",
        "The hotel offers complimentary breakfast for guests.",
        "He took a detour to avoid the traffic jam.",
        "She got a promotion at work last month.",
        "The festival is held annually in the town square.",
        "I can't find my phone charger anywhere.",
        "They were amazed by the view from the top of the mountain.",
        "She enjoys listening to jazz music in her free time.",
        "The train was delayed due to bad weather.",
        "He installed solar panels on his roof last summer.",
        "The children are building a sandcastle at the beach.",
        "She forgot to send the email attachment.",
        "They spent the afternoon kayaking on the lake.",
        "He prefers tea over coffee in the morning.",
        "The bookstore has a sale on science fiction novels.",
        "The fire alarm went off during the meeting.",
        "She loves watching sunsets by the ocean.",
        "They are planning a road trip across the country.",
        "He borrowed a book from the library about astronomy.",
        "The market is busy on Saturday mornings.",
        "She enrolled in an online coding course.",
        "He enjoys playing chess with his grandfather.",
        "The bakery runs out of croissants early in the morning.",
        "They moved into their new apartment last week.",
        "I can't believe how fast time flies.",
        "He forgot to water the plants for a week.",
        "The playground is crowded with children.",
        "She signed up for a pottery class.",
        "The restaurant serves the best sushi in town.",
        "They found a wallet on the sidewalk.",
        "The baby is sleeping peacefully in the crib.",
        "He needs to submit the report by Friday.",
        "The birds are chirping outside the window.",
        "She bought a bouquet of roses for her mother.",
        "He is learning how to play the violin.",
        "They are preparing for the winter holiday season.",
        "I forgot my password again.",
        "She enjoys painting landscapes in her spare time.",
        "He is working on a new mobile app project.",
        "The lake is frozen during the winter months.",
        "They are hosting a barbecue in their backyard.",
        "She likes visiting art galleries on weekends.",
        "He is writing a novel about space exploration.",
        "The city is known for its vibrant nightlife.",
        "She found a stray dog and took it home.",
        "They celebrated their anniversary at a fancy restaurant.",
        "He enjoys hiking through dense forests.",
        "She planted tomatoes and basil in her garden.",
        "The marathon takes place every April.",
        "The school offers classes in music and art.",
    ];
    let doc2 = documents.clone();
    // Generate embeddings with the default batch size, 256
    let embeddings = model.embed(documents, None).unwrap();

    println!("Embeddings length: {}", embeddings.len()); // -> Embeddings length: 4
    println!("Embedding dimension: {}", embeddings[0].len()); // -> Embedding dimension: 384

    for (i, v) in embeddings.iter().enumerate() {
        let mut d: Document = Document::new();
        d.add_text(field_id_content, doc2[i]);
        let v6 = Vector::from_slice(v, d);
        collection.add(v6).await.unwrap();
        // std::thread::sleep(std::time::Duration::from_secs(1));
    }

    // // let handle2 = thread::spawn(move || {
    // let test_doc = vec!["What is a good Italian restaurant in the area?"];
    // let embeddings = model.embed(test_doc, None).unwrap();
    // for _ in 0..1 {
    //     for (i, v) in embeddings.iter().enumerate() {
    //         let tensor = Tensor::arr_slice(v);
    //         let seacher = collection.searcher().await.unwrap();
    //         let v = seacher.query(&tensor, 5, None).unwrap();
    //         for vs in v.iter() {
    //             let vc = seacher.vector(vs).unwrap();
    //             println!("content:{:?}", vc.doc().get_field_values())
    //         }
    //         seacher.done();
    //         std::thread::sleep(std::time::Duration::from_secs(1));
    //     }
    // }
}

fn main() {
    let pool = Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("Failed to create runtime");

    let mut schema = Schema::with_vector(VectorEntry::new(
        "vector",
        AnnType::HNSW,
        TensorEntry::new(1, [384], VectorType::F32),
    ));
    let field_id_content = schema.add_field(FieldEntry::str("content"));
    let config = ConfigBuilder::default()
        .data_path(PathBuf::from("./data"))
        .collect_name("vector1")
        .build();

    let collection = Collection::new(schema, config).unwrap();
    let _id_field_id = collection.get_schema().get_field("_id").unwrap();
    pool.block_on(async move { test_query(_id_field_id).await });
    // pool.block_on(async move { test_insert(collection, field_id_content).await });
}
