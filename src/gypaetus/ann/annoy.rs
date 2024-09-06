use super::AnnIndex;
use crate::gypaetus::BinarySerialize;
use crate::gypaetus::Metric;
struct Annoy<V> {
    v: V,
}

// impl<V> BinarySerialize for Annoy<V> {
//     fn binary_deserialize<R: std::io::Read>(
//         reader: &mut R,
//     ) -> crate::gypaetus::util::error::GyResult<Self> {
//         todo!()
//     }

//     fn binary_serialize<W: std::io::Write>(
//         &self,
//         writer: &mut W,
//     ) -> crate::gypaetus::util::error::GyResult<()> {
//         todo!()
//     }
// }

// impl<V: BinarySerialize> AnnIndex<V> for Annoy<V>
// where
//     V: Metric<V>,
// {
//     fn insert(&mut self, q: V) -> crate::gypaetus::util::error::GyResult<usize> {
//         todo!()
//     }

//     fn query(
//         &self,
//         q: &V,
//         k: usize,
//     ) -> crate::gypaetus::util::error::GyResult<Vec<super::Neighbor>> {
//         todo!()
//     }

//     fn serialize<W: std::io::Write>(
//         &self,
//         writer: &mut W,
//     ) -> crate::gypaetus::util::error::GyResult<()> {
//         todo!()
//     }
// }
