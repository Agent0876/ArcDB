//! Heap file storage for ArcDB
//!
//! This module implements a simple heap file for storing tuples.
//! Tuples are stored sequentially without any particular order.

use serde::{Deserialize, Serialize};

use std::sync::{Arc, Mutex};

use super::buffer_pool::{BufferPoolManager, GlobalPageId};
use super::page::PageId;
use super::tuple::Tuple;
use crate::error::{Error, Result};

#[cfg(test)]
use super::tuple::Value;

/// A slot identifier (page_id, slot_number)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SlotId {
    pub page_id: PageId,
    pub slot_num: u16,
}

impl SlotId {
    pub fn new(page_id: PageId, slot_num: u16) -> Self {
        Self { page_id, slot_num }
    }
}

/// Heap file for storing tuples
#[derive(Debug)]
pub struct HeapFile {
    /// Table ID this heap file belongs to
    table_id: u32,
    /// Buffer pool manager
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
    /// First page ID
    _first_page_id: PageId,
    /// Last page ID (for fast insertion)
    last_page_id: PageId,
}

impl HeapFile {
    /// Create a new heap file
    pub fn new(table_id: u32, buffer_pool: Arc<Mutex<BufferPoolManager>>) -> Self {
        let (global_id, _) = {
            let mut bpm = buffer_pool.lock().unwrap();
            bpm.new_page(table_id).expect("Failed to create first page")
        };
        let first_page_id = global_id.page_id;

        // Unpin immediately as no-one is using it yet
        buffer_pool
            .lock()
            .unwrap()
            .unpin_page(global_id, false)
            .ok();

        Self {
            table_id,
            buffer_pool,
            _first_page_id: first_page_id,
            last_page_id: first_page_id,
        }
    }

    /// Open an existing heap file
    pub fn open(table_id: u32, buffer_pool: Arc<Mutex<BufferPoolManager>>) -> Result<Self> {
        let first_page_id = 0;
        let last_page_id = {
            let bpm = buffer_pool.lock().unwrap();
            let disk = bpm.disk_manager();
            disk.get_page_count(table_id)?.saturating_sub(1) as PageId
        };

        Ok(Self {
            table_id,
            buffer_pool,
            _first_page_id: first_page_id,
            last_page_id,
        })
    }

    /// Insert a tuple into the heap file
    pub fn insert(&mut self, tuple: Tuple) -> Result<SlotId> {
        let bytes = tuple.to_bytes();
        let page_id = self.last_page_id;
        let global_id = GlobalPageId {
            table_id: self.table_id,
            page_id,
        };

        let mut bpm = self.buffer_pool.lock().unwrap();
        let frame_index = bpm.fetch_page(global_id)?;

        let slot_num = {
            let page = bpm.get_page_mut(frame_index);
            page.insert_tuple(&bytes)
        };

        if let Some(sn) = slot_num {
            bpm.unpin_page(global_id, true)?;
            return Ok(SlotId::new(page_id, sn));
        }

        // Current page full, unpin and allocate new page
        bpm.unpin_page(global_id, false)?;

        let (new_global_id, new_frame_index) = bpm.new_page(self.table_id)?;
        self.last_page_id = new_global_id.page_id;

        let sn = {
            let page = bpm.get_page_mut(new_frame_index);
            page.insert_tuple(&bytes)
                .ok_or_else(|| Error::StorageError("Failed to insert into new page".to_string()))?
        };

        bpm.unpin_page(new_global_id, true)?;
        Ok(SlotId::new(new_global_id.page_id, sn))
    }

    /// Delete a tuple by slot ID
    pub fn delete(&mut self, slot_id: SlotId) -> Result<()> {
        let global_id = GlobalPageId {
            table_id: self.table_id,
            page_id: slot_id.page_id,
        };
        let mut bpm = self.buffer_pool.lock().unwrap();
        let frame_index = bpm.fetch_page(global_id)?;

        let success = {
            let page = bpm.get_page_mut(frame_index);
            page.delete_tuple(slot_id.slot_num)
        };

        bpm.unpin_page(global_id, success)?;
        if success {
            Ok(())
        } else {
            Err(Error::StorageError(format!(
                "Could not delete tuple at {:?}",
                slot_id
            )))
        }
    }

    /// Update a tuple by slot ID
    pub fn update(&mut self, slot_id: SlotId, tuple: Tuple) -> Result<()> {
        let bytes = tuple.to_bytes();
        let global_id = GlobalPageId {
            table_id: self.table_id,
            page_id: slot_id.page_id,
        };
        let mut bpm = self.buffer_pool.lock().unwrap();
        let frame_index = bpm.fetch_page(global_id)?;

        let success = {
            let page = bpm.get_page_mut(frame_index);
            page.update_tuple(slot_id.slot_num, &bytes)
        };

        bpm.unpin_page(global_id, success)?;
        if success {
            Ok(())
        } else {
            Err(Error::StorageError(format!(
                "Could not update tuple at {:?}",
                slot_id
            )))
        }
    }

    /// Get a tuple by slot ID
    pub fn get(&mut self, slot_id: SlotId) -> Option<Tuple> {
        let global_id = GlobalPageId {
            table_id: self.table_id,
            page_id: slot_id.page_id,
        };
        let mut bpm = self.buffer_pool.lock().unwrap();
        let frame_index = bpm.fetch_page(global_id).ok()?;

        let tuple = {
            let page = bpm.get_page(frame_index);
            let bytes = page.get_tuple(slot_id.slot_num)?;
            Tuple::from_bytes(bytes).ok()
        };

        bpm.unpin_page(global_id, false).ok();
        tuple
    }

    /// Scan all tuples
    pub fn scan(&mut self) -> Vec<(SlotId, Tuple)> {
        let mut result = Vec::new();
        let page_count = {
            let bpm = self.buffer_pool.lock().unwrap();
            bpm.disk_manager()
                .get_page_count(self.table_id)
                .unwrap_or(0) as PageId
        };

        for pid in 0..page_count {
            let global_id = GlobalPageId {
                table_id: self.table_id,
                page_id: pid,
            };
            let mut bpm = self.buffer_pool.lock().unwrap();
            if let Ok(index) = bpm.fetch_page(global_id) {
                let page = bpm.get_page(index);
                let count = page.tuple_count();
                for sn in 0..count as u16 {
                    if let Some(bytes) = page.get_tuple(sn) {
                        if let Ok(tuple) = Tuple::from_bytes(bytes) {
                            result.push((SlotId::new(pid, sn), tuple));
                        }
                    }
                }
                bpm.unpin_page(global_id, false).ok();
            }
        }
        result
    }

    /// Flush to disk
    pub fn flush(&mut self) -> Result<()> {
        let mut bpm = self.buffer_pool.lock().unwrap();
        bpm.flush_all()
    }

    /// Get LSN for a specific page
    pub fn get_page_lsn(&mut self, page_id: PageId) -> u64 {
        let global_id = GlobalPageId {
            table_id: self.table_id,
            page_id,
        };
        let mut bpm = self.buffer_pool.lock().unwrap();
        if let Ok(index) = bpm.fetch_page(global_id) {
            let lsn = bpm.get_page(index).lsn();
            bpm.unpin_page(global_id, false).ok();
            lsn
        } else {
            0
        }
    }

    /// Set LSN for a specific page
    pub fn set_page_lsn(&mut self, page_id: PageId, lsn: u64) {
        let global_id = GlobalPageId {
            table_id: self.table_id,
            page_id,
        };
        let mut bpm = self.buffer_pool.lock().unwrap();
        if let Ok(index) = bpm.fetch_page(global_id) {
            bpm.get_page_mut(index).set_lsn(lsn);
            bpm.unpin_page(global_id, true).ok();
        }
    }

    /// Get the number of tuples
    pub fn tuple_count(&mut self) -> usize {
        self.scan().len()
    }

    /// Get table ID
    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Clear all tuples (stub)
    pub fn clear(&mut self) {
        // In reality, should truncate file and reset page count.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::DiskManager;
    use std::path::PathBuf;

    fn setup_bpm() -> Arc<Mutex<BufferPoolManager>> {
        let data_dir = PathBuf::from("data_test_heap");
        if !data_dir.exists() {
            std::fs::create_dir(&data_dir).ok();
        }
        let disk = Arc::new(DiskManager::new(data_dir));
        Arc::new(Mutex::new(BufferPoolManager::new(10, disk)))
    }

    #[test]
    fn test_heap_file_insert() {
        let bpm = setup_bpm();
        let mut heap = HeapFile::new(1, bpm);

        let tuple = Tuple::new(vec![Value::Integer(1), Value::String("test".to_string())]);

        let slot_id = heap.insert(tuple.clone()).unwrap();

        assert_eq!(heap.tuple_count(), 1);
        assert_eq!(heap.get(slot_id), Some(tuple));
    }

    #[test]
    fn test_heap_file_delete() {
        let bpm = setup_bpm();
        let mut heap = HeapFile::new(1, bpm);

        let tuple = Tuple::new(vec![Value::Integer(1)]);
        let slot_id = heap.insert(tuple).unwrap();

        assert_eq!(heap.tuple_count(), 1);

        heap.delete(slot_id).unwrap();

        assert_eq!(heap.tuple_count(), 0);
        assert!(heap.get(slot_id).is_none());
    }

    #[test]
    fn test_heap_file_update() {
        let bpm = setup_bpm();
        let mut heap = HeapFile::new(1, bpm);

        let tuple1 = Tuple::new(vec![Value::Integer(1)]);
        let slot_id = heap.insert(tuple1).unwrap();

        let tuple2 = Tuple::new(vec![Value::Integer(2)]);
        heap.update(slot_id, tuple2.clone()).unwrap();

        assert_eq!(heap.get(slot_id), Some(tuple2));
    }

    #[test]
    fn test_heap_file_scan() {
        let bpm = setup_bpm();
        let mut heap = HeapFile::new(1, bpm);

        for i in 0..5 {
            heap.insert(Tuple::new(vec![Value::Integer(i)])).unwrap();
        }

        let tuples = heap.scan();
        assert_eq!(tuples.len(), 5);
    }
}
