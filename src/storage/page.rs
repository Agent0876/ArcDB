//! Page management for ArcDB
//!
//! This module defines the page structure used for disk-based storage.
//! Each page is a fixed-size block (4KB by default) that can store tuples.

/// Page size in bytes (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Page header size
pub const PAGE_HEADER_SIZE: usize = 24;

/// Page ID type
pub type PageId = u32;

/// Special page ID for invalid/unallocated pages
pub const INVALID_PAGE_ID: PageId = u32::MAX;

/// Page header structure
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    /// Page ID
    pub page_id: PageId,
    /// Number of tuples in this page
    pub tuple_count: u16,
    /// Offset to free space start
    pub free_space_offset: u16,
    /// Page type (0 = data, 1 = index, etc.)
    pub page_type: u8,
    /// Last LSN that modified this page
    pub lsn: u64,
    /// Reserved for future use
    pub reserved: [u8; 5],
}

impl PageHeader {
    /// Create a new page header
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            tuple_count: 0,
            free_space_offset: PAGE_SIZE as u16,
            page_type: 0,
            lsn: 0,
            reserved: [0; 5],
        }
    }

    /// Get free space available
    pub fn free_space(&self) -> usize {
        self.free_space_offset as usize - PAGE_HEADER_SIZE - (self.tuple_count as usize * 4)
    }
}

/// A database page
#[derive(Debug, Clone)]
pub struct Page {
    /// Page header
    header: PageHeader,
    /// Raw page data
    data: Vec<u8>,
    /// Is this page dirty (modified)?
    dirty: bool,
    /// Pin count (number of users holding this page)
    pin_count: u32,
}

impl Page {
    /// Create a new empty page
    pub fn new(page_id: PageId) -> Self {
        let mut page = Self {
            header: PageHeader::new(page_id),
            data: vec![0u8; PAGE_SIZE],
            dirty: false,
            pin_count: 0,
        };
        page.write_header();
        page.dirty = false; // Reset dirty flag after initial header write
        page
    }

    /// Write header to the data buffer
    fn write_header(&mut self) {
        self.data[0..4].copy_from_slice(&self.header.page_id.to_le_bytes());
        self.data[4..6].copy_from_slice(&self.header.tuple_count.to_le_bytes());
        self.data[6..8].copy_from_slice(&self.header.free_space_offset.to_le_bytes());
        self.data[8] = self.header.page_type;
        self.data[9..17].copy_from_slice(&self.header.lsn.to_le_bytes());
        // Skip reserved [17..24] for now or zero them
        self.data[17..24].fill(0);
        self.mark_dirty();
    }

    /// Set page LSN
    pub fn set_lsn(&mut self, lsn: u64) {
        self.header.lsn = lsn;
        self.write_header();
    }

    /// Create a page from raw bytes
    pub fn from_bytes(page_id: PageId, bytes: &[u8]) -> Self {
        let mut data = vec![0u8; PAGE_SIZE];
        let len = bytes.len().min(PAGE_SIZE);
        data[..len].copy_from_slice(&bytes[..len]);

        // Parse header from bytes
        let tuple_count = u16::from_le_bytes([data[4], data[5]]);
        let free_space_offset = u16::from_le_bytes([data[6], data[7]]);
        let page_type = data[8];
        let mut lsn_bytes = [0u8; 8];
        lsn_bytes.copy_from_slice(&data[9..17]);
        let lsn = u64::from_le_bytes(lsn_bytes);

        let header = PageHeader {
            page_id,
            tuple_count,
            free_space_offset,
            page_type,
            lsn,
            reserved: [0; 5],
        };

        Self {
            header,
            data,
            dirty: false,
            pin_count: 0,
        }
    }

    /// Get page ID
    pub fn page_id(&self) -> PageId {
        self.header.page_id
    }

    /// Get page LSN
    pub fn lsn(&self) -> u64 {
        self.header.lsn
    }

    /// Get tuple count
    pub fn tuple_count(&self) -> usize {
        self.header.tuple_count as usize
    }

    /// Get free space
    pub fn free_space(&self) -> usize {
        self.header.free_space()
    }

    /// Check if page is dirty
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark page as dirty
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Clear dirty flag
    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    /// Set dirty flag
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Get pin count
    pub fn pin_count(&self) -> u32 {
        self.pin_count
    }

    /// Increment pin count
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Decrement pin count
    pub fn unpin(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    /// Get raw data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable raw data
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        &mut self.data
    }

    /// Serialize page to bytes
    pub fn to_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Insert a tuple into the page
    /// Returns the slot index if successful
    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> Option<u16> {
        let size = tuple_data.len();
        if self.free_space() < size + 4 {
            return None;
        }

        let slot_num = self.header.tuple_count;
        let offset = self.header.free_space_offset as usize - size;

        // Update header
        self.header.tuple_count += 1;
        self.header.free_space_offset = offset as u16;

        // Write slot entry (offset [2 bytes], size [2 bytes])
        let slot_offset = PAGE_HEADER_SIZE + (slot_num as usize * 4);
        self.data[slot_offset..slot_offset + 2].copy_from_slice(&(offset as u16).to_le_bytes());
        self.data[slot_offset + 2..slot_offset + 4].copy_from_slice(&(size as u16).to_le_bytes());

        // Write tuples data
        self.data[offset..offset + size].copy_from_slice(tuple_data);

        self.write_header();
        Some(slot_num)
    }

    /// Update a tuple in the page
    pub fn update_tuple(&mut self, slot_num: u16, tuple_data: &[u8]) -> bool {
        if slot_num >= self.header.tuple_count {
            return false;
        }

        let slot_offset = PAGE_HEADER_SIZE + (slot_num as usize * 4);
        let old_offset =
            u16::from_le_bytes([self.data[slot_offset], self.data[slot_offset + 1]]) as usize;
        let old_size =
            u16::from_le_bytes([self.data[slot_offset + 2], self.data[slot_offset + 3]]) as usize;

        let new_size = tuple_data.len();

        if new_size <= old_size {
            // Can update in place
            // (Strictly speaking, we could shift data to reclaim space, but ArcDB keeps it simple)
            self.data[old_offset..old_offset + new_size].copy_from_slice(tuple_data);
            self.data[slot_offset + 2..slot_offset + 4]
                .copy_from_slice(&(new_size as u16).to_le_bytes());
            self.mark_dirty();
            true
        } else {
            // Cannot update in place if larger, unless we shift everything.
            // For ArcDB, let's just fail or implement a simple "move to new location"
            // but that's complex because we need to update the slot.
            // Simplified: if it fits in free space, move it.
            if self.free_space() >= new_size {
                let offset = self.header.free_space_offset as usize - new_size;
                self.header.free_space_offset = offset as u16;
                self.data[offset..offset + new_size].copy_from_slice(tuple_data);

                self.data[slot_offset..slot_offset + 2]
                    .copy_from_slice(&(offset as u16).to_le_bytes());
                self.data[slot_offset + 2..slot_offset + 4]
                    .copy_from_slice(&(new_size as u16).to_le_bytes());

                self.write_header();
                true
            } else {
                false
            }
        }
    }

    /// Delete a tuple from the page
    pub fn delete_tuple(&mut self, slot_num: u16) -> bool {
        if slot_num >= self.header.tuple_count {
            return false;
        }

        let slot_offset = PAGE_HEADER_SIZE + (slot_num as usize * 4);
        // Mark as deleted by setting size to 0
        self.data[slot_offset + 2..slot_offset + 4].copy_from_slice(&0u16.to_le_bytes());
        self.mark_dirty();
        true
    }

    /// Get a tuple from the page by slot index
    pub fn get_tuple(&self, slot_num: u16) -> Option<&[u8]> {
        if slot_num >= self.header.tuple_count {
            return None;
        }

        let slot_offset = PAGE_HEADER_SIZE + (slot_num as usize * 4);
        let offset =
            u16::from_le_bytes([self.data[slot_offset], self.data[slot_offset + 1]]) as usize;
        let size =
            u16::from_le_bytes([self.data[slot_offset + 2], self.data[slot_offset + 3]]) as usize;

        if size == 0 {
            return None; // Deleted tuple
        }

        Some(&self.data[offset..offset + size])
    }
}

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// In-memory page storage with disk persistence
#[derive(Debug)]
pub struct PageStorage {
    /// Pages stored in memory (cache)
    pages: std::collections::HashMap<PageId, Page>,
    /// Next page ID to allocate
    next_page_id: PageId,
    /// Disk file for persistence
    file: Option<File>,
}

impl PageStorage {
    /// Create a new in-memory page storage
    pub fn new() -> Self {
        Self {
            pages: std::collections::HashMap::new(),
            next_page_id: 0,
            file: None,
        }
    }

    /// Open or create a disk-backed page storage
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let next_page_id = (metadata.len() / PAGE_SIZE as u64) as PageId;

        Ok(Self {
            pages: std::collections::HashMap::new(),
            next_page_id,
            file: Some(file),
        })
    }

    /// Allocate a new page
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;

        let mut page = Page::new(page_id);
        page.mark_dirty();

        if let Some(ref mut file) = self.file {
            let offset = page_id as u64 * PAGE_SIZE as u64;
            file.seek(SeekFrom::Start(offset)).unwrap();
            file.write_all(page.to_bytes()).unwrap();
            page.clear_dirty();
        }

        self.pages.insert(page_id, page);
        page_id
    }

    /// Get a page by ID
    pub fn get_page(&mut self, page_id: PageId) -> Option<&Page> {
        if !self.pages.contains_key(&page_id) {
            if let Some(ref mut file) = self.file {
                let offset = page_id as u64 * PAGE_SIZE as u64;
                if offset < file.metadata().unwrap().len() {
                    let mut bytes = vec![0u8; PAGE_SIZE];
                    file.seek(SeekFrom::Start(offset)).unwrap();
                    file.read_exact(&mut bytes).unwrap();
                    let page = Page::from_bytes(page_id, &bytes);
                    self.pages.insert(page_id, page);
                }
            }
        }
        self.pages.get(&page_id)
    }

    /// Get a mutable page by ID
    pub fn get_page_mut(&mut self, page_id: PageId) -> Option<&mut Page> {
        if !self.pages.contains_key(&page_id) {
            self.get_page(page_id);
        }
        self.pages.get_mut(&page_id)
    }

    /// Flush all dirty pages to disk
    pub fn flush(&mut self) -> std::io::Result<()> {
        if let Some(ref mut file) = self.file {
            for (&page_id, page) in self.pages.iter_mut() {
                if page.is_dirty() {
                    let offset = page_id as u64 * PAGE_SIZE as u64;
                    file.seek(SeekFrom::Start(offset))?;
                    file.write_all(page.to_bytes())?;
                    page.clear_dirty();
                }
            }
            file.flush()?;
        }
        Ok(())
    }

    /// Free a page (mark for deletion - simplified)
    pub fn free_page(&mut self, page_id: PageId) -> bool {
        self.pages.remove(&page_id).is_some()
    }

    /// Get number of pages
    pub fn page_count(&self) -> usize {
        self.next_page_id as usize
    }
}

impl Default for PageStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(0);
        assert_eq!(page.page_id(), 0);
        assert_eq!(page.tuple_count(), 0);
        assert!(!page.is_dirty());
    }

    #[test]
    fn test_page_dirty_flag() {
        let mut page = Page::new(0);
        assert!(!page.is_dirty());

        page.mark_dirty();
        assert!(page.is_dirty());

        page.clear_dirty();
        assert!(!page.is_dirty());
    }

    #[test]
    fn test_page_storage() {
        let mut storage = PageStorage::new();

        let page_id = storage.allocate_page();
        assert_eq!(page_id, 0);

        let page = storage.get_page(page_id);
        assert!(page.is_some());
        assert_eq!(page.unwrap().page_id(), 0);

        assert!(storage.free_page(page_id));
        assert!(storage.get_page(page_id).is_none());
    }
}
