use crate::storage::{Direction, LedgerStorageBackend};
use many::ManyError;
use merk::tree::Tree;
use merk::Merk;
use merk::{rocksdb, BatchEntry, Op};
use std::borrow::Cow;
use std::collections::Bound::*;
use std::collections::{BTreeMap, Bound};
use std::path::Path;

fn incr(mut v: Vec<u8>) -> Vec<u8> {
    for x in (0..v.len()).rev() {
        if v[x] != 0xFF {
            v[x] += 1;
            return v;
        }
    }

    // If we make it here, the vector is entirely `0xFFFFFFFFFFFF...` so
    // adding one is creating a new vector `0x1000000000000...`.
    // We could also panic here but that doesn't really make sense.
    let mut tmp = vec![0; v.len() + 1];
    tmp[0] = 1;
    tmp
}

/// Storage backend which uses Merk as the persistent store.
/// This is Sync and Send, even though the ledger module isn't (yet).
/// It also allows for re-entry.
pub struct MerkStorageBackend {
    merk: Merk,
    stage: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MerkStorageBackend {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, ManyError> {
        Ok(Self {
            merk: Merk::open(path).map_err(|e| ManyError::unknown(e.to_string()))?,
            stage: BTreeMap::new(),
        })
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ManyError> {
        Self::create(path)
    }
}

impl LedgerStorageBackend for MerkStorageBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ManyError> {
        self.merk
            .get(key)
            .map_err(|e| ManyError::unknown(e.to_string()))
    }

    fn get_staged(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ManyError> {
        if let Some(b) = self.stage.get(key) {
            return Ok(Some(b.clone()));
        }
        self.get(key)
    }

    fn hash(&self) -> Vec<u8> {
        self.merk.root_hash().to_vec()
    }

    fn commit(&mut self) -> Result<(), ManyError> {
        let batch: Vec<BatchEntry> = self
            .stage
            .iter()
            .map(|(k, v)| (k.clone(), Op::Put(v.clone())))
            .collect();
        self.stage = BTreeMap::new();

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
        self.stage.insert(key.to_vec(), value);
    }

    fn iter<'a>(
        &'a self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        direction: Direction,
    ) -> Box<dyn Iterator<Item = (Cow<[u8]>, Cow<[u8]>)> + 'a> {
        use rocksdb::{IteratorMode, ReadOptions};

        // Set the iterator bounds.
        let mut options = ReadOptions::default();
        let mode = match direction {
            Direction::Forward => {
                match start {
                    Included(s) => options.set_iterate_lower_bound(s),
                    Excluded(s) => options.set_iterate_lower_bound(incr(s)),
                    Unbounded => {}
                }
                match end {
                    Included(e) => options.set_iterate_upper_bound(incr(e)),
                    Excluded(e) => options.set_iterate_upper_bound(e),
                    Unbounded => {}
                }
                IteratorMode::Start
            }
            Direction::Backward => {
                match start {
                    Included(s) => options.set_iterate_lower_bound(s),
                    Excluded(s) => options.set_iterate_lower_bound(incr(s)),
                    Unbounded => {}
                }
                match end {
                    Included(e) => options.set_iterate_upper_bound(incr(e)),
                    Excluded(e) => options.set_iterate_upper_bound(e),
                    Unbounded => {}
                }
                IteratorMode::End
            }
        };

        let it = Merk::iter_opt(&self.merk, mode, options);
        let it = it.map(|(k, v)| {
            let v = Tree::decode(k.to_vec(), v.as_ref());
            (Cow::from(k.to_vec()), Cow::from(v.value().to_vec()))
        });

        Box::new(it)
    }
}

#[test]
fn iterator_works() {
    fn iter(db: &MerkStorageBackend, start: Bound<u64>, end: Bound<u64>, d: Direction) -> Vec<u64> {
        fn to_vec(b: Bound<u64>) -> Bound<Vec<u8>> {
            match b {
                Included(s) => Included(s.to_be_bytes().to_vec()),
                Excluded(s) => Excluded(s.to_be_bytes().to_vec()),
                Unbounded => Unbounded,
            }
        }
        db.iter(to_vec(start), to_vec(end), d)
            .map(|(_, v)| {
                let mut s = [0u8; 8];
                s.copy_from_slice(v.as_ref());
                u64::from_be_bytes(s)
            })
            .collect::<Vec<u64>>()
    }

    fn check(db: &MerkStorageBackend, start: Bound<u64>, end: Bound<u64>, mut expected: Vec<u64>) {
        assert_eq!(&iter(db, start, end, Direction::Forward), &expected);
        expected.reverse();
        assert_eq!(&iter(db, start, end, Direction::Backward), &expected);
    }

    let persistent_path = tempfile::tempdir().unwrap();
    let mut db = MerkStorageBackend::create(persistent_path).unwrap();

    for i in 1..=5 {
        let b = u64::to_be_bytes(i).to_vec();
        db.put(b.clone(), b);
    }
    db.commit().unwrap();

    check(&db, Included(2), Included(4), vec![2, 3, 4]);
    check(&db, Included(2), Excluded(4), vec![2, 3]);
    check(&db, Excluded(2), Included(4), vec![3, 4]);
    check(&db, Excluded(2), Excluded(4), vec![3]);
    check(&db, Included(2), Unbounded, vec![2, 3, 4, 5]);
    check(&db, Excluded(2), Unbounded, vec![3, 4, 5]);
    check(&db, Unbounded, Included(4), vec![1, 2, 3, 4]);
    check(&db, Unbounded, Excluded(4), vec![1, 2, 3]);
    check(&db, Unbounded, Unbounded, vec![1, 2, 3, 4, 5]);
}
