// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crc::{Crc, CRC_64_MS};
use memmap2::{Mmap, MmapOptions};
use minibytes::Bytes;
use parking_lot::Mutex;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{IoSlice, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;

pub struct WalWriter {
    file: File,
    pos: u64,
}

// todo(andrey)
// - iteration
pub struct WalReader {
    fd: RawFd,
    maps: Mutex<BTreeMap<u64, Bytes>>,
}

#[derive(Clone, Copy)]
pub struct WalPosition {
    start: u64,
}

pub fn walf(mut file: File) -> io::Result<(WalWriter, WalReader)> {
    file.seek(SeekFrom::End(0))?;
    make_wal(file)
}

pub fn wal(path: impl AsRef<Path>) -> io::Result<(WalWriter, WalReader)> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    make_wal(file)
}

fn make_wal(file: File) -> io::Result<(WalWriter, WalReader)> {
    let fd = unsafe { libc::dup(file.as_raw_fd()) };
    if fd <= 0 {
        return Err(io::Error::last_os_error());
    }
    let reader = WalReader {
        fd,
        maps: Default::default(),
    };
    let writer = WalWriter {
        pos: file.metadata()?.len(),
        file,
    };
    Ok((writer, reader))
}

const MAP_SIZE: u64 = 0x10_000;
// 16 pages
const MAP_MASK: u64 = !0xffff;
const ZERO_MAP: [u8; MAP_SIZE as usize] = [0u8; MAP_SIZE as usize];
const _: () = assert_constants();

const CRC: Crc<u64> = Crc::<u64>::new(
    &CRC_64_MS, /*selection of algorithm here is mostly random*/
);
const HEADER_LEN_BYTES: u64 = 8 + 8;
// CRC and length
const HEADER_LEN_BYTES_USIZE: usize = HEADER_LEN_BYTES as usize;

const fn assert_constants() {
    if u64::MAX - MAP_MASK != MAP_SIZE - 1 {
        panic!("MAP_MASK and MAP_SIZE do not match");
    }
}

fn offset(p: u64) -> u64 {
    p & MAP_MASK
}

impl WalWriter {
    pub fn write(&mut self, b: &[u8]) -> io::Result<WalPosition> {
        let len = b.len() as u64 + HEADER_LEN_BYTES;
        assert!(len <= MAP_SIZE);
        let mut buffs = vec![];
        let mut written_expected = 0usize;
        tracing::trace!(
            "pos={}, len={}, self.pos + len - 1={}, a(pos)={}, a(pos+len)={}",
            self.pos,
            len,
            self.pos + len - 1,
            offset(self.pos),
            offset(self.pos + len - 1)
        );
        if offset(self.pos) != offset(self.pos + len - 1) {
            let extra_len = offset(self.pos + len - 1) - self.pos;
            let extra = &ZERO_MAP[0..(extra_len as usize)];
            buffs.push(IoSlice::new(extra));
            written_expected += extra.len();
            self.pos += extra_len;
            debug_assert_eq!(offset(self.pos), self.pos);
            debug_assert_eq!(offset(self.pos), offset(self.pos + len - 1));
        }
        let crc = CRC.checksum(b);
        let header = combine_crc_and_len(crc, len);
        let header = header.to_le_bytes();
        buffs.push(IoSlice::new(&header));
        buffs.push(IoSlice::new(b));
        written_expected += len as usize;
        let written = self.file.write_vectored(&buffs)?;
        assert_eq!(written, written_expected);
        let position = WalPosition { start: self.pos };
        self.pos += len;
        Ok(position)
    }
}

fn combine_crc_and_len(crc: u64, len: u64) -> u128 {
    crc as u128 + ((len as u128) << 64)
}

fn split_crc_and_len(combined: u128) -> (u64, u64) {
    let crc = (combined & ((1 << 64) - 1)) as u64;
    let len = (combined >> 64) as u64;
    (crc, len)
}

impl WalReader {
    pub fn read(&self, position: WalPosition) -> io::Result<Bytes> {
        match self.try_read(position)? {
            Some(entry) => Ok(entry),
            None => panic!("No entry found at position {}", position.start),
        }
    }

    fn try_read(&self, position: WalPosition) -> io::Result<Option<Bytes>> {
        let offset = offset(position.start);
        let bytes = self.map_offset(offset)?;
        let buf_offset = (position.start - offset) as usize;
        let (crc, len) = Self::read_header(&bytes[buf_offset..]);
        if len == 0 {
            if crc == 0 {
                return Ok(None);
            }
            panic!(
                "Non-zero crc at len 0, crc: {crc}, position:{}",
                position.start
            );
        }
        let bytes = bytes.slice(buf_offset + HEADER_LEN_BYTES_USIZE..buf_offset + (len as usize));
        let actual_crc = CRC.checksum(bytes.as_ref());
        if actual_crc != crc {
            // todo - return error
            panic!(
                "Crc mismatch, expected {}, found {} at position {}:{}",
                crc, actual_crc, position.start, len
            );
        }
        Ok(Some(bytes))
    }

    // Attempts cleaning internal mem maps, returning number of retained maps
    // Map can be freed when all buffers linked to this portion of a file are dropped
    pub fn cleanup(&self) -> usize {
        let mut maps = self.maps.lock();
        maps.retain(|_k, v| v.downcast_mut::<Mmap>().is_none());
        maps.len()
    }

    // Iter all entries up to writer position at the time iter_until(...) is called
    pub fn iter_until(&self, w: &WalWriter) -> WalIterator {
        WalIterator {
            wal_reader: self,
            position: Some(WalPosition { start: 0 }),
            end_position: w.pos,
        }
    }

    fn map_offset(&self, offset: u64) -> io::Result<Bytes> {
        let mut maps = self.maps.lock();
        let bytes = match maps.entry(offset) {
            Entry::Vacant(va) => {
                let mmap = unsafe {
                    MmapOptions::new()
                        .offset(offset)
                        .len(MAP_SIZE as usize)
                        .map(self.fd)?
                };
                va.insert(mmap.into())
            }
            Entry::Occupied(oc) => oc.into_mut(),
        };
        Ok(bytes.clone())
    }

    #[inline]
    fn read_header(bytes: &[u8]) -> (u64, u64 /*(crc, len)*/) {
        let mut header = [0u8; HEADER_LEN_BYTES_USIZE];
        header.copy_from_slice(&bytes[..HEADER_LEN_BYTES_USIZE]);
        let header = u128::from_le_bytes(header);
        split_crc_and_len(header)
    }
}

pub struct WalIterator<'a> {
    wal_reader: &'a WalReader,
    position: Option<WalPosition>,
    end_position: u64,
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = (WalPosition, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        let position = self.position.take()?;
        tracing::trace!("Iter read {}", position.start);
        // Either read from current position, or try next mapping, but only once
        if let Some(item) = self.try_position(position) {
            return Some(item);
        }
        if position.first_in_map() {
            return None;
        }
        tracing::trace!("Iter fallback read {}", position.next_start_offset().start);
        // todo - need to consider crash recovery here
        // Either need to reset writer position, or read all offsets until writer position
        self.try_position(position.next_start_offset())
    }
}

impl<'a> WalIterator<'a> {
    fn try_position(&mut self, position: WalPosition) -> Option<(WalPosition, Bytes)> {
        if position.start >= self.end_position {
            return None;
        }
        let entry = self
            .wal_reader
            .try_read(position)
            .expect("Failed to read wal")?;
        self.position = Some(position.add(entry.len() as u64 + HEADER_LEN_BYTES));
        Some((position, entry))
    }
}

impl WalPosition {
    pub fn add(&self, len: u64) -> Self {
        Self {
            start: self.start + len,
        }
    }

    fn next_start_offset(&self) -> Self {
        let offset = offset(self.start);
        Self {
            start: offset + MAP_SIZE,
        }
    }

    fn first_in_map(&self) -> bool {
        self.start == offset(self.start)
    }
}

impl Drop for WalReader {
    fn drop(&mut self) {
        unsafe {
            if libc::close(self.fd) != 0 {
                Err::<(), _>(io::Error::last_os_error()).expect("Failed to close wal fd");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal() {
        let temp = tempdir::TempDir::new("test_wal").unwrap();
        let file = temp.path().join("wal");
        let (mut writer, reader) = wal(&file).unwrap();
        let one = [1u8; 1024];
        let two = [2u8; (MAP_SIZE - HEADER_LEN_BYTES) as usize];
        let three = [3u8; 15];
        let four = [4u8; 18];
        let one_pos = writer.write(&one).unwrap();
        let two_pos = writer.write(&two).unwrap();
        let three_pos = writer.write(&three).unwrap();

        assert_eq!(&one, reader.read(one_pos).unwrap().as_ref());
        assert_eq!(&two, reader.read(two_pos).unwrap().as_ref());
        assert_eq!(&three, reader.read(three_pos).unwrap().as_ref());
        drop(reader);
        drop(writer);

        let (mut writer, reader) = wal(&file).unwrap();
        assert_eq!(&one, reader.read(one_pos).unwrap().as_ref());
        assert_eq!(&two, reader.read(two_pos).unwrap().as_ref());
        assert_eq!(&three, reader.read(three_pos).unwrap().as_ref());

        let four_pos = writer.write(&four).unwrap();
        assert_eq!(&one, reader.read(one_pos).unwrap().as_ref());
        assert_eq!(&two, reader.read(two_pos).unwrap().as_ref());
        assert_eq!(&three, reader.read(three_pos).unwrap().as_ref());
        assert_eq!(&four, reader.read(four_pos).unwrap().as_ref());

        drop(reader);
        drop(writer);

        let (writer, reader) = wal(&file).unwrap();
        drop(writer);

        // Can create new mappings when writer is dropped
        assert_eq!(&one, reader.read(one_pos).unwrap().as_ref());
        assert_eq!(&two, reader.read(two_pos).unwrap().as_ref());
        assert_eq!(&three, reader.read(three_pos).unwrap().as_ref());
        assert_eq!(&four, reader.read(four_pos).unwrap().as_ref());

        // Verify that we can free unused mappings
        let one_read = reader.read(one_pos).unwrap();
        assert_eq!(reader.cleanup(), 1);
        assert_eq!(&one, one_read.as_ref());
        drop(one_read);
        assert_eq!(reader.cleanup(), 0);
        drop(reader);

        let (writer, reader) = wal(&file).unwrap();

        let mut iter = reader.iter_until(&writer);
        drop(writer);

        assert_eq!(&one, iter.next().unwrap().1.as_ref());
        assert_eq!(&two, iter.next().unwrap().1.as_ref());
        assert_eq!(&three, iter.next().unwrap().1.as_ref());
        assert_eq!(&four, iter.next().unwrap().1.as_ref());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_wal_read_after_write() {
        // Verify that mapping can read entries that are written to it after it is created
        let temp = tempdir::TempDir::new("test_wal").unwrap();
        let file = temp.path().join("wal");
        let (mut writer, reader) = wal(&file).unwrap();
        let one = [1u8; 15];
        let two = [2u8; 18];
        let one_pos = writer.write(&one).unwrap();
        let one_read = reader.read(one_pos).unwrap();
        assert_eq!(&one, one_read.as_ref());

        let two_pos = writer.write(&two).unwrap();
        let two_read = reader.read(two_pos).unwrap();
        assert_eq!(&two, two_read.as_ref());

        assert_eq!(1, reader.cleanup()); // assert only one mapping was created (therefore one and two share same mapping)
    }

    #[test]
    fn combine_crc_len_test() {
        for crc in [0, 1, 12, u64::MAX] {
            for len in [0, 1, 18, u64::MAX] {
                let combined = combine_crc_and_len(crc, len);
                assert_eq!((crc, len), split_crc_and_len(combined));
            }
        }
    }
}
