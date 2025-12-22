//! Temporary storage for delta objects while their base object is still decoding, keyed by both pack
//! offset and object hash.

use dashmap::DashMap;

use crate::{hash::ObjectHash, internal::pack::cache_object::CacheObject};

/// Waitlist for Delta objects while the Base object is not ready.
/// Easier and faster than Channels.
#[derive(Default, Debug)]
pub struct Waitlist {
    //TODO Memory Control!
    pub map_offset: DashMap<usize, Vec<CacheObject>>,
    pub map_ref: DashMap<ObjectHash, Vec<CacheObject>>,
}

impl Waitlist {
    /// Create a new, empty Waitlist.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert an object into the waitlist by its pack offset or object hash.
    pub fn insert_offset(&self, offset: usize, obj: CacheObject) {
        self.map_offset.entry(offset).or_default().push(obj);
    }

    /// Insert an object into the waitlist by its object hash.
    pub fn insert_ref(&self, hash: ObjectHash, obj: CacheObject) {
        self.map_ref.entry(hash).or_default().push(obj);
    }

    /// Take objects out (get & remove)
    /// <br> Return Vec::new() if None
    pub fn take(&self, offset: usize, hash: ObjectHash) -> Vec<CacheObject> {
        let mut res = Vec::new();
        if let Some((_, vec)) = self.map_offset.remove(&offset) {
            res.extend(vec);
        }
        if let Some((_, vec)) = self.map_ref.remove(&hash) {
            res.extend(vec);
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal::{object::types::ObjectType, pack::cache_object::CacheObjectInfo};

    /// Helper to build a base CacheObject with given size and hash.
    fn make_test_obj(offset: usize) -> CacheObject {
        CacheObject {
            info: CacheObjectInfo::BaseObject(ObjectType::Blob, ObjectHash::default()),
            offset,
            crc32: 0,
            data_decompressed: vec![],
            mem_recorder: None,
            is_delta_in_pack: false,
        }
    }

    /// Test inserting and taking objects by offset.
    #[test]
    fn test_waitlist_offset() {
        let waitlist = Waitlist::new();
        let obj1 = make_test_obj(10);
        let obj2 = make_test_obj(20);

        waitlist.insert_offset(100, obj1);
        waitlist.insert_offset(100, obj2);

        let res = waitlist.take(100, ObjectHash::default());
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].offset, 10);
        assert_eq!(res[1].offset, 20);

        let res_empty = waitlist.take(100, ObjectHash::default());
        assert!(res_empty.is_empty());
    }

    /// Test inserting and taking objects by object hash.
    #[test]
    fn test_waitlist_ref() {
        let waitlist = Waitlist::new();
        let hash = ObjectHash::new(b"test_hash");
        let obj = make_test_obj(30);

        waitlist.insert_ref(hash, obj);

        let res = waitlist.take(0, hash);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].offset, 30);

        let res_empty = waitlist.take(0, hash);
        assert!(res_empty.is_empty());
    }

    /// Test inserting and taking objects by both offset and object hash.
    #[test]
    fn test_waitlist_mixed() {
        let waitlist = Waitlist::new();
        let hash = ObjectHash::new(b"test_hash");
        let offset = 200;

        let obj1 = make_test_obj(1);
        let obj2 = make_test_obj(2);

        waitlist.insert_offset(offset, obj1);
        waitlist.insert_ref(hash, obj2);

        // Take using both keys, should retrieve both lists
        let res = waitlist.take(offset, hash);
        assert_eq!(res.len(), 2);

        // Verify we got both objects
        assert!(res.iter().any(|o| o.offset == 1));
        assert!(res.iter().any(|o| o.offset == 2));

        // Verify maps are empty
        assert!(waitlist.map_offset.is_empty());
        assert!(waitlist.map_ref.is_empty());
    }
}
