use super::AnnIndex;
use crate::BinarySerialize;
use crate::Metric;
struct Annoy<V> {
    v: V,
}

// impl<V> BinarySerialize for Annoy<V> {
//     fn binary_deserialize<R: std::io::Read>(
//         reader: &mut R,
//     ) -> crate::util::error::GyResult<Self> {
//         todo!()
//     }

//     fn binary_serialize<W: std::io::Write>(
//         &self,
//         writer: &mut W,
//     ) -> crate::util::error::GyResult<()> {
//         todo!()
//     }
// }

// impl<V: BinarySerialize> AnnIndex<V> for Annoy<V>
// where
//     V: Metric<V>,
// {
//     fn insert(&mut self, q: V) -> crate::util::error::GyResult<usize> {
//         todo!()
//     }

//     fn query(
//         &self,
//         q: &V,
//         k: usize,
//     ) -> crate::util::error::GyResult<Vec<super::Neighbor>> {
//         todo!()
//     }

//     fn serialize<W: std::io::Write>(
//         &self,
//         writer: &mut W,
//     ) -> crate::util::error::GyResult<()> {
//         todo!()
//     }
// }
