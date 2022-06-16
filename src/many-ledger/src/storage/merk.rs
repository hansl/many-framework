use crate::storage::LedgerStorageBackend;
use many::ManyError;
use merk::tree::Tree;
use merk::Merk;
use merk::{rocksdb, BatchEntry, Op};
use std::collections::BTreeMap;
use std::path::Path;

/// Storage backend which uses Merk as the persistent store.
/// This is Sync and Send, even though the ledger module isn't (yet).
/// It also allows for re-entry.
pub struct MerkStorageBackend {
    merk: Merk,
    batch: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MerkStorageBackend {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, ManyError> {
        Ok(Self {
            merk: Merk::open(path).map_err(|e| ManyError::unknown(e.to_string()))?,
            batch: BTreeMap::new(),
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ManyError> {
        Self::create(path)
    }
}

impl LedgerStorageBackend for MerkStorageBackend {
    fn get(&self, key: &[u8], check_stage: bool) -> Result<Option<Vec<u8>>, ManyError> {
        if check_stage {
            if let Some(b) = self.batch.get(key) {
                return Ok(Some(b.clone()));
            }
        }

        self.merk
            .get(key)
            .map_err(|e| ManyError::unknown(e.to_string()))
    }

    fn hash(&self) -> Vec<u8> {
        self.merk.root_hash().to_vec()
    }

    fn commit(&mut self) -> Result<(), ManyError> {
        let batch: Vec<BatchEntry> = self
            .batch
            .into_iter()
            .map(|(k, v)| (k, Op::Put(v)))
            .collect();

        self.batch = BTreeMap::new();

        // Batch is sorted and unique at this point, so we can use the unchecked apply.
        unsafe {
            self.merk
                .apply_unchecked(&batch)
                .map_err(|e| ManyError::unknown(e.to_string()))?;
        }
        self.merk
            .commit(&[])
            .map_err(|e| ManyError::unknown(e.to_string()))
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.batch.insert(key.to_vec(), value);
    }
}
