pub struct HNSW {
    enter_point: u32,
}

struct Neighbor {
    id: u32,
    d: f32, //distance
}

impl HNSW {
    fn get_random_level() {}

    fn insert(&mut self, q: Vec<f32>, id: u32) {
        //let cur_level =
    }

    fn search_at_layer(q: Vec<f32>, level: u32) {
        let candidates: Vec<Neighbor> = Vec::new();
    }

    fn search() {}
}

#[cfg(test)]
mod tests {
    use rand::{thread_rng, Rng};

    #[test]
    fn test_hnsw() {
        let mut rng = rand::thread_rng();
        let x: f64 = rng.gen();
        println!("x = {x}");
    }
}
