//! B+ Tree Index for ArcDB
//!
//! This module implements a B+ tree index for efficient key-value lookups.
//! The B+ tree is a self-balancing tree data structure that maintains sorted data
//! and allows searches, sequential access, insertions, and deletions in O(log n) time.

use std::cmp::Ordering;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use super::buffer_pool::BufferPoolManager;
use super::heap::SlotId;
use super::tuple::Value;
use crate::error::{Error, Result};
use std::sync::{Arc, Mutex};

const ORDER: usize = 4;

/// A key in the B+ tree (wraps Value for comparison)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexKey(pub Vec<Value>);

impl IndexKey {
    /// Create a new index key from a single value
    pub fn new(value: Value) -> Self {
        Self(vec![value])
    }

    /// Create a new composite index key
    pub fn composite(values: Vec<Value>) -> Self {
        Self(values)
    }

    /// Compare two index keys
    pub fn compare(&self, other: &IndexKey) -> Ordering {
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            match a.compare(b) {
                Some(Ordering::Equal) => continue,
                Some(ord) => return ord,
                None => return Ordering::Equal,
            }
        }
        self.0.len().cmp(&other.0.len())
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.compare(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.compare(other)
    }
}

impl Eq for IndexKey {}

/// B+ Tree Node
#[derive(Debug, Clone, Serialize, Deserialize)]
enum BPlusNode {
    /// Internal node with keys and child pointers
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<Box<BPlusNode>>,
    },
    /// Leaf node with keys and record pointers
    Leaf {
        keys: Vec<IndexKey>,
        values: Vec<SlotId>,
        next: Option<Box<BPlusNode>>, // For range scans
    },
}

/// B+ Tree Index
#[derive(Debug, Serialize, Deserialize)]
pub struct BPlusTree {
    /// Root node
    root: Option<Box<BPlusNode>>,
    /// Number of entries
    size: usize,
    /// Index name
    pub name: String,
    /// Buffer pool manager (not used yet, but propagated)
    #[serde(skip)]
    _buffer_pool: Option<Arc<Mutex<BufferPoolManager>>>,
}

impl BPlusTree {
    /// Create a new empty B+ tree
    pub fn new(name: impl Into<String>, buffer_pool: Arc<Mutex<BufferPoolManager>>) -> Self {
        Self {
            root: None,
            size: 0,
            name: name.into(),
            _buffer_pool: Some(buffer_pool),
        }
    }

    /// Save the tree to disk
    pub fn save_to_disk(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let file = std::fs::File::create(path).map_err(|e| Error::Internal(e.to_string()))?;
        serde_json::to_writer(file, self).map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }

    /// Load the tree from disk
    pub fn load_from_disk(
        path: impl AsRef<std::path::Path>,
        buffer_pool: Arc<Mutex<BufferPoolManager>>,
    ) -> Result<Self> {
        let file = std::fs::File::open(path).map_err(|e| Error::Internal(e.to_string()))?;
        let mut tree: BPlusTree =
            serde_json::from_reader(file).map_err(|e| Error::Internal(e.to_string()))?;
        tree._buffer_pool = Some(buffer_pool);
        Ok(tree)
    }

    /// Insert a key-value pair into the tree
    pub fn insert(&mut self, key: IndexKey, value: SlotId) -> Result<()> {
        if self.root.is_none() {
            self.root = Some(Box::new(BPlusNode::Leaf {
                keys: vec![key],
                values: vec![value],
                next: None,
            }));
            self.size += 1;
            return Ok(());
        }

        let mut root = self.root.take().unwrap();
        if let Some((new_node, mid_key)) = self.insert_recursive(&mut root, key, value) {
            // Root split, create new root
            self.root = Some(Box::new(BPlusNode::Internal {
                keys: vec![mid_key],
                children: vec![root, new_node],
            }));
        } else {
            self.root = Some(root);
        }

        self.size += 1;
        Ok(())
    }

    fn insert_recursive(
        &mut self,
        node: &mut BPlusNode,
        key: IndexKey,
        value: SlotId,
    ) -> Option<(Box<BPlusNode>, IndexKey)> {
        match node {
            BPlusNode::Leaf { keys, values, .. } => {
                let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
                keys.insert(pos, key);
                values.insert(pos, value);

                if keys.len() > ORDER {
                    let mid = keys.len() / 2;
                    let new_keys = keys.split_off(mid);
                    let new_values = values.split_off(mid);
                    let mid_key = new_keys[0].clone();

                    return Some((
                        Box::new(BPlusNode::Leaf {
                            keys: new_keys,
                            values: new_values,
                            next: None, // Simplified next pointer logic
                        }),
                        mid_key,
                    ));
                }
                None
            }
            BPlusNode::Internal { keys, children } => {
                let pos = keys.binary_search(&key).unwrap_or_else(|e| e);
                if let Some((new_node, mid_key)) =
                    self.insert_recursive(&mut children[pos], key, value)
                {
                    keys.insert(pos, mid_key);
                    children.insert(pos + 1, new_node);

                    if keys.len() > ORDER {
                        let mid = keys.len() / 2;
                        let mid_key = keys[mid].clone();
                        let new_keys = keys.split_off(mid + 1);
                        keys.pop(); // Remove mid_key from left node
                        let new_children = children.split_off(mid + 1);

                        return Some((
                            Box::new(BPlusNode::Internal {
                                keys: new_keys,
                                children: new_children,
                            }),
                            mid_key,
                        ));
                    }
                }
                None
            }
        }
    }

    /// Search for a key in the tree
    pub fn search(&self, key: &IndexKey) -> Option<SlotId> {
        let mut curr = self.root.as_ref()?;
        loop {
            match curr.as_ref() {
                BPlusNode::Leaf { keys, values, .. } => {
                    if let Ok(pos) = keys.binary_search(key) {
                        return Some(values[pos]);
                    }
                    return None;
                }
                BPlusNode::Internal { keys, children } => {
                    // For exact match, go right (pos+1); for non-match, go to insertion point
                    let pos = match keys.binary_search(key) {
                        Ok(p) => p + 1, // Exact match: go to right child
                        Err(p) => p,    // Not found: go to appropriate child
                    };
                    curr = &children[pos];
                }
            }
        }
    }

    /// Delete a key from the tree
    pub fn delete(&mut self, key: &IndexKey) -> Result<Option<SlotId>> {
        if self.root.is_none() {
            return Ok(None);
        }

        // Simplified deletion: just find and remove from leaf
        // In a real B+ Tree, this would involve merging and redistributing nodes.
        let mut result = None;
        let mut root = self.root.take().unwrap();
        if let Some(r) = self.delete_recursive(&mut root, key) {
            result = Some(r);
        }
        self.root = Some(root);

        if result.is_some() {
            self.size -= 1;
        }
        Ok(result)
    }

    fn delete_recursive(&mut self, node: &mut BPlusNode, key: &IndexKey) -> Option<SlotId> {
        match node {
            BPlusNode::Leaf { keys, values, .. } => {
                if let Ok(pos) = keys.binary_search(key) {
                    keys.remove(pos);
                    return Some(values.remove(pos));
                }
                None
            }
            BPlusNode::Internal { keys, children } => {
                let pos = match keys.binary_search(key) {
                    Ok(p) => p + 1,
                    Err(p) => p,
                };
                self.delete_recursive(&mut children[pos], key)
            }
        }
    }

    /// Range scan: find all keys in [start, end]
    pub fn range_scan(
        &self,
        start: Option<&IndexKey>,
        end: Option<&IndexKey>,
    ) -> Vec<(IndexKey, SlotId)> {
        let mut result = Vec::new();
        if let Some(root) = &self.root {
            self.range_scan_recursive(root, start, end, &mut result);
        }
        result
    }

    fn range_scan_recursive(
        &self,
        node: &BPlusNode,
        start: Option<&IndexKey>,
        end: Option<&IndexKey>,
        result: &mut Vec<(IndexKey, SlotId)>,
    ) {
        match node {
            BPlusNode::Leaf { keys, values, .. } => {
                for (i, key) in keys.iter().enumerate() {
                    let too_small = start.map_or(false, |s| key < s);
                    let too_large = end.map_or(false, |e| key > e);
                    if !too_small && !too_large {
                        result.push((key.clone(), values[i]));
                    }
                }
            }
            BPlusNode::Internal { keys, children } => {
                // Find start child
                let start_pos = start.map_or(0, |s| keys.binary_search(s).unwrap_or_else(|e| e));
                // Find end child
                let end_pos =
                    end.map_or(keys.len(), |e| keys.binary_search(e).unwrap_or_else(|e| e));

                for i in start_pos..=end_pos {
                    self.range_scan_recursive(&children[i], start, end, result);
                }
            }
        }
    }

    /// Get all entries in the tree (sorted)
    pub fn scan_all(&self) -> Vec<(IndexKey, SlotId)> {
        self.range_scan(None, None)
    }

    /// Number of entries in the tree
    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageId;

    fn setup_bpm() -> Arc<Mutex<BufferPoolManager>> {
        let data_dir = std::path::PathBuf::from("data_test_btree");
        if !data_dir.exists() {
            std::fs::create_dir(&data_dir).ok();
        }
        let disk = Arc::new(crate::storage::disk::DiskManager::new(data_dir));
        Arc::new(Mutex::new(BufferPoolManager::new(10, disk)))
    }

    fn make_key(i: i32) -> IndexKey {
        IndexKey::new(Value::Integer(i))
    }

    fn make_slot(p: PageId, s: u16) -> SlotId {
        SlotId::new(p, s)
    }

    #[test]
    fn test_btree_insert_and_search() {
        let bpm = setup_bpm();
        let mut tree = BPlusTree::new("test_index", bpm);

        tree.insert(make_key(5), make_slot(0, 5)).unwrap();
        tree.insert(make_key(3), make_slot(0, 3)).unwrap();
        tree.insert(make_key(7), make_slot(0, 7)).unwrap();
        tree.insert(make_key(1), make_slot(0, 1)).unwrap();

        assert_eq!(tree.search(&make_key(5)), Some(make_slot(0, 5)));
        assert_eq!(tree.search(&make_key(3)), Some(make_slot(0, 3)));
        assert_eq!(tree.search(&make_key(7)), Some(make_slot(0, 7)));
        assert_eq!(tree.search(&make_key(1)), Some(make_slot(0, 1)));
        assert_eq!(tree.search(&make_key(99)), None);
    }

    #[test]
    fn test_btree_delete() {
        let bpm = setup_bpm();
        let mut tree = BPlusTree::new("test_index", bpm);

        for i in 1..=5 {
            tree.insert(make_key(i), make_slot(0, i as u16)).unwrap();
        }

        let deleted = tree.delete(&make_key(3)).unwrap();
        assert_eq!(deleted, Some(make_slot(0, 3)));
        assert_eq!(tree.search(&make_key(3)), None);
    }

    #[test]
    fn test_btree_range_scan() {
        let bpm = setup_bpm();
        let mut tree = BPlusTree::new("test_index", bpm);

        for i in [1, 3, 5, 7, 9, 11, 13, 15] {
            tree.insert(make_key(i), make_slot(0, i as u16)).unwrap();
        }

        let results = tree.range_scan(Some(&make_key(5)), Some(&make_key(11)));
        assert_eq!(results.len(), 4); // 5, 7, 9, 11
    }

    #[test]
    fn test_btree_many_inserts() {
        let bpm = setup_bpm();
        let mut tree = BPlusTree::new("test_index", bpm);

        for i in 0..20 {
            tree.insert(make_key(i), make_slot(0, i as u16)).unwrap();
        }

        for i in 0..20 {
            assert_eq!(tree.search(&make_key(i)), Some(make_slot(0, i as u16)));
        }
    }

    #[test]
    fn test_btree_scan_all() {
        let bpm = setup_bpm();
        let mut tree = BPlusTree::new("test_index", bpm);

        for i in [5, 2, 8, 1, 9, 3] {
            tree.insert(make_key(i), make_slot(0, i as u16)).unwrap();
        }

        let all = tree.scan_all();
        assert_eq!(all.len(), 6);

        let keys: Vec<i32> = all
            .iter()
            .map(|(k, _)| {
                if let Value::Integer(v) = &k.0[0] {
                    *v
                } else {
                    0
                }
            })
            .collect();

        assert_eq!(keys, vec![1, 2, 3, 5, 8, 9]);
    }
}
