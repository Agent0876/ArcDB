//! Disk manager for ArcDB
//!
//! This module handles direct file I/O for multiple tables.

use crate::error::Result;
use crate::storage::page::{PageId, PAGE_SIZE};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Disk manager
#[derive(Debug)]
pub struct DiskManager {
    /// Mapping from table_id to its file path
    table_files: Mutex<HashMap<u32, PathBuf>>,
    /// File handles for open tables
    open_files: Mutex<HashMap<u32, File>>,
    /// Directory where data files are stored
    data_dir: PathBuf,
}

impl DiskManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            table_files: Mutex::new(HashMap::new()),
            open_files: Mutex::new(HashMap::new()),
            data_dir,
        }
    }

    pub fn register_table(&self, table_id: u32, path: impl AsRef<Path>) {
        let mut table_files = self.table_files.lock().unwrap();
        table_files.insert(table_id, path.as_ref().to_path_buf());
    }

    pub fn read_page(&self, table_id: u32, page_id: PageId, data: &mut [u8]) -> Result<()> {
        let mut open_files = self.open_files.lock().unwrap();
        let file = self.get_file_mut(&mut open_files, table_id)?;
        file.seek(SeekFrom::Start((page_id as u64) * (PAGE_SIZE as u64)))?;
        file.read_exact(data)?;
        Ok(())
    }

    pub fn write_page(&self, table_id: u32, page_id: PageId, data: &[u8]) -> Result<()> {
        let mut open_files = self.open_files.lock().unwrap();
        let file = self.get_file_mut(&mut open_files, table_id)?;
        file.seek(SeekFrom::Start((page_id as u64) * (PAGE_SIZE as u64)))?;
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }

    /// Allocate a new page on disk and return its ID
    pub fn allocate_page(&self, table_id: u32) -> Result<PageId> {
        let mut open_files = self.open_files.lock().unwrap();
        let file = self.get_file_mut(&mut open_files, table_id)?;
        let file_len = file.metadata()?.len();
        let page_id = (file_len / PAGE_SIZE as u64) as PageId;

        // Extend file by one page
        file.seek(SeekFrom::End(0))?;
        let zero_page = vec![0u8; PAGE_SIZE];
        file.write_all(&zero_page)?;
        file.flush()?;

        Ok(page_id)
    }

    pub fn get_page_count(&self, table_id: u32) -> Result<u64> {
        let mut open_files = self.open_files.lock().unwrap();
        let file = self.get_file_mut(&mut open_files, table_id)?;
        let file_len = file.metadata()?.len();
        Ok(file_len / PAGE_SIZE as u64)
    }

    fn get_file_mut<'a>(
        &self,
        open_files: &'a mut HashMap<u32, File>,
        table_id: u32,
    ) -> Result<&'a mut File> {
        if !open_files.contains_key(&table_id) {
            let table_files = self.table_files.lock().unwrap();
            let path = table_files
                .get(&table_id)
                .cloned()
                .unwrap_or_else(|| self.data_dir.join(format!("table_{}.data", table_id)));

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?;
            open_files.insert(table_id, file);
        }
        Ok(open_files.get_mut(&table_id).unwrap())
    }
}
