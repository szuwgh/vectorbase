pub struct HNSW {}

struct Neighbor {
    id: u32,
    d: f32, //distance
}

impl HNSW {
    fn insert(q: Vec<f32>, id: u32) {}

    fn search_at_layer(q: Vec<f32>, level: u32) {
        let candidates: Vec<Neighbor> = Vec::new();
    }

    fn search() {}
}
