use crate::{layout, util};

pub struct IndexTable {
    entries: Vec<layout::IndexTableEntry>,
}

impl IndexTable {
    pub fn new(header: &layout::CSOHeader) -> Self {
        Self {
            entries: vec![layout::IndexTableEntry::default(); header.index_table_len()],
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn deserialize(data: Vec<u8>) -> Self {
        let len = data.len() / 4;
        let mut index_table = Self {
            entries: vec![layout::IndexTableEntry::default(); len],
        };

        for idx in 0..len {
            let start_byte = 4 * idx;
            let end_byte = start_byte + 4;

            let entry = util::deserialize_u32_le(&data[start_byte..end_byte]);
            index_table[idx] = layout::IndexTableEntry::new_with_raw_value(entry);
        }

        index_table
    }

    pub fn serialize(&self) -> Box<[u8]> {
        let mut output = vec![0; 4 * self.len()];
        for idx in 0..self.len() {
            let start_byte = 4 * idx;
            let end_byte = start_byte + 4;

            let bytes = self[idx].raw_value().to_le_bytes();
            output[start_byte..end_byte].copy_from_slice(&bytes);
        }

        output.into_boxed_slice()
    }
}

impl core::ops::Index<usize> for IndexTable {
    type Output = layout::IndexTableEntry;

    fn index(&self, index: usize) -> &Self::Output {
        &self.entries[index]
    }
}

impl core::ops::IndexMut<usize> for IndexTable {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.entries[index]
    }
}
