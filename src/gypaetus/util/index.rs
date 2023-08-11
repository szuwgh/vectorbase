use art_tree::Art;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;
pub trait CacheIndex<K, V> {
    fn contains_key(&self, k: &K) -> bool;
    fn insert(&mut self, k: K, v: V) -> Option<V>;
    fn get(&self, k: &K) -> Option<&V>;
}

///```
/// let mut art = Art::<u16, u16>::new();
/// for i in 0..u8::MAX as u16 {
///     assert!(art.insert(i, i), "{}", i);
///     assert!(matches!(art.get(&i), Some(val) if val == &i));
/// }
/// for i in 0..u8::MAX as u16 {
///     assert!(matches!(art.remove(&i), Some(val) if val == i));
/// }
/// let mut art = Art::<ByteString, u16>::new();
/// for i in 0..u8::MAX as u16 {
///     let key = KeyBuilder::new().append(i).append(ByteString::new("abc".to_string().as_bytes())).build();
///     art.upsert(key.clone(), i + 1);
///     assert!(matches!(art.get(&key), Some(val) if val == &(i + 1)));
/// }
/// let from_key = KeyBuilder::new().append(16u16).append(ByteString::new("abc".to_string().as_bytes())).build();
/// let until_key = KeyBuilder::new().append(20u16).append(ByteString::new("abc".to_string().as_bytes())).build();
/// assert_eq!(art.range(from_key..=until_key).count(), 5);
/// assert_eq!(art.iter().count(), u8::MAX as usize);
///```

pub(crate) struct ART<K, V> {
    cache: Art<K, V>,
}

impl<K, V> CacheIndex<K, V> for ART<K, V> {
    fn contains_key(&self, k: &K) -> bool {
        false
    }
    fn insert(&mut self, k: K, v: V) -> Option<V> {
        None
    }

    fn get(&self, k: &K) -> Option<&V> {
        None
    }
}

pub(crate) struct AdaptiveRadixTree {}
