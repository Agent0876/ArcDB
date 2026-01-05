//! Buffer pool manager for ArcDB
//!
//! This module implements a fixed-size buffer pool for caching pages from disk.
//! It uses an LRU-based eviction policy.

use std::collections::HashMap;
use std::sync::Arc;

use super::disk::DiskManager;
use super::page::{Page, PageId, PAGE_SIZE};
use crate::error::{Error, Result};

/// A global page identifier (table_id, page_id)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlobalPageId {
    pub table_id: u32,
    pub page_id: PageId,
}

/// Buffer pool manager
#[derive(Debug)]
pub struct BufferPoolManager {
    /// Maximum number of pages in the buffer pool
    _pool_size: usize,
    /// Page table: GlobalPageId -> Buffer index
    page_table: HashMap<GlobalPageId, usize>,
    /// Buffer frames
    frames: Vec<Page>,
    /// Frame identifiers (None if frame is empty)
    frame_ids: Vec<Option<GlobalPageId>>,
    /// Free list (indices of available frames)
    free_list: Vec<usize>,
    /// LRU Replacer (list of indices in order of use, least recent first)
    replacer: Vec<usize>,
    /// Disk manager for file I/O
    disk_manager: Arc<DiskManager>,
}

impl BufferPoolManager {
    pub fn new(pool_size: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut frame_ids = Vec::with_capacity(pool_size);
        let mut free_list = Vec::with_capacity(pool_size);

        for i in 0..pool_size {
            frames.push(Page::new(0)); // Dummy page_id, will be updated
            frame_ids.push(None);
            free_list.push(i);
        }

        Self {
            _pool_size: pool_size,
            page_table: HashMap::new(),
            frames,
            frame_ids,
            free_list,
            replacer: Vec::new(),
            disk_manager,
        }
    }

    pub fn fetch_page(&mut self, global_id: GlobalPageId) -> Result<usize> {
        if let Some(&index) = self.page_table.get(&global_id) {
            self.pin_page(index);
            return Ok(index);
        }

        let index = self.get_victim_frame()?;

        // If victim was a valid page, remove from page table
        if let Some(old_id) = self.frame_ids[index] {
            self.page_table.remove(&old_id);
        }

        // Read page from disk
        let mut data = vec![0u8; PAGE_SIZE];
        self.disk_manager
            .read_page(global_id.table_id, global_id.page_id, &mut data)?;

        // Update frame
        self.frames[index] = Page::from_bytes(global_id.page_id, &data);
        self.frame_ids[index] = Some(global_id);
        self.page_table.insert(global_id, index);
        self.pin_page(index);

        Ok(index)
    }

    pub fn new_page(&mut self, table_id: u32) -> Result<(GlobalPageId, usize)> {
        let page_id = self.disk_manager.allocate_page(table_id)?;
        let global_id = GlobalPageId { table_id, page_id };

        let index = self.get_victim_frame()?;

        if let Some(old_id) = self.frame_ids[index] {
            self.page_table.remove(&old_id);
        }

        self.frames[index] = Page::new(page_id);
        self.frames[index].set_dirty(true);
        self.frame_ids[index] = Some(global_id);
        self.page_table.insert(global_id, index);
        self.pin_page(index);

        Ok((global_id, index))
    }

    pub fn unpin_page(&mut self, global_id: GlobalPageId, is_dirty: bool) -> Result<()> {
        if let Some(&index) = self.page_table.get(&global_id) {
            if is_dirty {
                self.frames[index].set_dirty(true);
            }
            self.frames[index].unpin();
            if self.frames[index].pin_count() == 0 {
                self.replacer.push(index);
            }
            Ok(())
        } else {
            Err(Error::Internal("Page not in buffer pool".to_string()))
        }
    }

    pub fn get_page(&self, index: usize) -> &Page {
        &self.frames[index]
    }

    pub fn get_page_mut(&mut self, index: usize) -> &mut Page {
        &mut self.frames[index]
    }

    fn pin_page(&mut self, index: usize) {
        self.frames[index].pin();
        if let Some(pos) = self.replacer.iter().position(|&x| x == index) {
            self.replacer.remove(pos);
        }
    }

    pub fn flush_page(&mut self, global_id: GlobalPageId) -> Result<()> {
        if let Some(&index) = self.page_table.get(&global_id) {
            if self.frames[index].is_dirty() {
                self.disk_manager.write_page(
                    global_id.table_id,
                    global_id.page_id,
                    &self.frames[index].to_bytes(),
                )?;
                self.frames[index].set_dirty(false);
            }
            Ok(())
        } else {
            Err(Error::Internal("Page not in buffer pool".to_string()))
        }
    }

    pub fn flush_all(&mut self) -> Result<()> {
        let ids: Vec<GlobalPageId> = self.frame_ids.iter().flatten().cloned().collect();
        for id in ids {
            self.flush_page(id)?;
        }
        Ok(())
    }

    fn get_victim_frame(&mut self) -> Result<usize> {
        if let Some(index) = self.free_list.pop() {
            return Ok(index);
        }

        if self.replacer.is_empty() {
            return Err(Error::Internal("Buffer pool overflow".to_string()));
        }

        let index = self.replacer.remove(0);
        if let Some(global_id) = self.frame_ids[index] {
            self.flush_page(global_id)?;
        }

        Ok(index)
    }

    pub fn get_global_id_for_frame(&self, index: usize) -> Option<GlobalPageId> {
        self.frame_ids[index]
    }

    pub fn disk_manager(&self) -> Arc<DiskManager> {
        self.disk_manager.clone()
    }
}
