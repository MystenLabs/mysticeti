// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use memmap2::{Mmap, MmapOptions};
use minibytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{IoSlice, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::{fmt, io};

pub struct WalWriter {
    file: File,
    pos: u64,
}

pub struct WalReader {
    fd: RawFd,
    maps: Mutex<BTreeMap<u64, Bytes>>,
}

pub struct WalSyncer {
    file: File,
}

#[derive(
    Clone, Copy, Eq, PartialEq, Debug, Ord, PartialOrd, Hash, Default, Serialize, Deserialize,
)]
pub struct WalPosition {
    start: u64,
}

pub type Tag = u32;

pub fn walf(mut file: File) -> io::Result<(WalWriter, WalReader)> {
    file.seek(SeekFrom::End(0))?;
    make_wal(file)
}

/// Opens file with mode suitable for walf
pub fn open_file_for_wal(p: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(p)
}

/// Creates wal reader and wal writer on the file.
/// WalWriter methods generally take &mut references so normally only one thread can access WalWriter at a time.
/// WalReader methods take & reference, you can wrap WalReader in Arc and safely share it across different threads.
///
/// You can write to wal using WalWriter::write method, which takes byte slice and a 'tag'(an opaque u32 value).
/// WalWriter::write method returns WalPosition that can later be used to read the wal.
///
/// When WalWriter::write completes, the data is guaranteed to be written into kernel buffers, but not synced to disk.
/// If the program terminates early, the data will be eventually written to disk by the operating system.
/// If the machine crashes however, some data might be lost, unless WalWriter::sync method is called to flush data to disk.
///
///
/// You can read the wal using WalReader::read by providing the WalPosition returned by WalWriter.
///
/// WalReader::read uses memory mapped files to map a region of the wal, and returns a pointer(in the form of Bytes abstraction)
/// pointing to the region of the memory mapped file.
///
/// All returned Bytes references(and their sub-slices) holds a reference counter to the underlining memory mapped file.
/// This allows to minimize copy of data, however it also means that holding the references to the buffers returned by WalReader::read will keep the wal file open.
///
/// In order to completely "close" the wal file(for example to allow to delete/reclaim space), user need to drop
/// the wal reader, wal writer, **and** drop all the buffers returned by WalReader::read().
#[allow(dead_code)]
pub fn wal(path: impl AsRef<Path>) -> io::Result<(WalWriter, WalReader)> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    make_wal(file)
}

fn make_wal(file: File) -> io::Result<(WalWriter, WalReader)> {
    // todo - replace dup with File::try_clone()
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

#[cfg(not(test))]
const MAP_SIZE: u64 = 0x10_000_000;
#[cfg(not(test))]
const MAP_MASK: u64 = !0xfffffff;
#[cfg(test)]
const MAP_SIZE: u64 = 0x10_000; // 16 pages
#[cfg(test)]
const MAP_MASK: u64 = !0xffff;
const ZERO_MAP: [u8; MAP_SIZE as usize] = [0u8; MAP_SIZE as usize];
const _: () = assert_constants();

pub const MAX_ENTRY_SIZE: usize = (MAP_SIZE - HEADER_LEN_BYTES) as usize;

// todo - we still allocate 64 bits for crc in wal header, reconsider it
const HEADER_LEN_BYTES: u64 = 8 + 8;
// CRC and length
const HEADER_LEN_BYTES_USIZE: usize = HEADER_LEN_BYTES as usize;

#[allow(dead_code)]
const fn assert_constants() {
    if u64::MAX - MAP_MASK != MAP_SIZE - 1 {
        panic!("MAP_MASK and MAP_SIZE do not match");
    }
    // Checks mask is in form 1...10....0
    check_zeroes(MAP_MASK);
}

const fn check_zeroes(m: u64) {
    if m & 1 == 1 {
        check_ones(m >> 1);
    } else {
        check_zeroes(m >> 1);
    }
}

const fn check_ones(m: u64) {
    if m == 0 {
        return;
    }
    if m & 1 != 1 {
        panic!("Invalid mask");
    }
    check_ones(m >> 1);
}

fn offset(p: u64) -> u64 {
    p & MAP_MASK
}

impl WalWriter {
    pub fn write(&mut self, tag: Tag, b: &[u8]) -> io::Result<WalPosition> {
        self.writev(tag, &[IoSlice::new(b)])
    }

    pub fn writev(&mut self, tag: Tag, v: &[IoSlice]) -> io::Result<WalPosition> {
        let v_len = v.iter().map(|s| s.len()).sum::<usize>();
        let len = v_len as u64 + HEADER_LEN_BYTES;
        assert!(len <= MAP_SIZE, "Wal entry too big, {len} < {MAP_SIZE}");
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
        let mut crc = crc32fast::Hasher::new();
        for slice in v {
            crc.update(slice);
        }
        let crc = crc.finalize() as u64;
        let header = combine_header(crc, len, tag);
        let header = header.to_le_bytes();
        buffs.push(IoSlice::new(&header));
        buffs.extend_from_slice(v);
        written_expected += len as usize;
        let written = self.file.write_vectored(&buffs)?;
        assert_eq!(written, written_expected);
        let position = WalPosition { start: self.pos };
        self.pos += len;
        Ok(position)
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    /// Allow to retrieve a 'syncer' instance that allows
    /// to fsync wal to disk without acquiring a lock on wal itself.
    ///
    /// In mysticeti specifically this allows to have an independent syncer thread that
    /// does not share locks with consensus thread.
    pub fn syncer(&self) -> io::Result<WalSyncer> {
        let file = self.file.try_clone()?;
        Ok(WalSyncer { file })
    }
}

impl WalSyncer {
    pub fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}

fn combine_header(crc: u64, len: u64, tag: Tag) -> u128 {
    // wal entries are currently limited by (much smaller) MAP_SIZE
    assert!(len <= u32::MAX as u64);

    crc as u128 + ((len as u128) << 64) + ((tag as u128) << (64 + 32))
}

fn split_header(combined: u128) -> (u64, u64, Tag) {
    let crc = (combined & ((1 << 64) - 1)) as u64;
    let len = ((combined >> 64) & ((1 << 32) - 1)) as u64;
    let tag = (combined >> (64 + 32)) as Tag;
    (crc, len, tag)
}

impl WalReader {
    pub fn read(&self, position: WalPosition) -> io::Result<(Tag, Bytes)> {
        match self.try_read(position)? {
            Some(entry) => Ok(entry),
            None => panic!("No entry found at position {}", position.start),
        }
    }

    fn try_read(&self, position: WalPosition) -> io::Result<Option<(Tag, Bytes)>> {
        let offset = offset(position.start);
        let bytes = self.map_offset(offset)?;
        let buf_offset = (position.start - offset) as usize;
        let (crc, len, tag) = Self::read_header(&bytes[buf_offset..]);
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
        let actual_crc = crc32fast::hash(bytes.as_ref()) as u64;
        if actual_crc != crc {
            // todo - return error
            panic!(
                "Crc mismatch, expected {}, found {} at position {}:{}",
                crc, actual_crc, position.start, len
            );
        }
        Ok(Some((tag, bytes)))
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
    fn read_header(bytes: &[u8]) -> (u64, u64, Tag /*(crc, len, tag)*/) {
        let mut header = [0u8; HEADER_LEN_BYTES_USIZE];
        header.copy_from_slice(&bytes[..HEADER_LEN_BYTES_USIZE]);
        let header = u128::from_le_bytes(header);
        split_header(header)
    }
}

pub struct WalIterator<'a> {
    wal_reader: &'a WalReader,
    position: Option<WalPosition>,
    end_position: u64,
}

impl<'a> Iterator for WalIterator<'a> {
    type Item = (WalPosition, (Tag, Bytes));

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
    fn try_position(&mut self, position: WalPosition) -> Option<(WalPosition, (Tag, Bytes))> {
        if position.start >= self.end_position {
            return None;
        }
        let (tag, data) = self
            .wal_reader
            .try_read(position)
            .expect("Failed to read wal")?;
        self.position = Some(position.add(data.len() as u64 + HEADER_LEN_BYTES));
        Some((position, (tag, data)))
    }
}

impl WalPosition {
    pub const MAX: WalPosition = WalPosition { start: u64::MAX };

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
                #[allow(clippy::unnecessary_literal_unwrap)]
                Err::<(), _>(io::Error::last_os_error()).expect("Failed to close wal fd");
            }
        }
    }
}

impl fmt::Display for WalPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.start)
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
        let one_tag = 5;
        let two_tag = 10;
        let three_tag = 15;
        let four_tag = 20;
        let one_pos = writer.write(one_tag, &one).unwrap();
        let two_pos = writer.write(two_tag, &two).unwrap();
        let three_pos = writer.write(three_tag, &three).unwrap();

        assert_eq!(&one, rd(&reader, one_pos, one_tag).as_ref());
        assert_eq!(&two, rd(&reader, two_pos, two_tag).as_ref());
        assert_eq!(&three, rd(&reader, three_pos, three_tag).as_ref());
        drop(reader);
        drop(writer);

        let (mut writer, reader) = wal(&file).unwrap();
        assert_eq!(&one, rd(&reader, one_pos, one_tag).as_ref());
        assert_eq!(&two, rd(&reader, two_pos, two_tag).as_ref());
        assert_eq!(&three, rd(&reader, three_pos, three_tag).as_ref());

        let four_pos = writer.write(four_tag, &four).unwrap();
        assert_eq!(&one, rd(&reader, one_pos, one_tag).as_ref());
        assert_eq!(&two, rd(&reader, two_pos, two_tag).as_ref());
        assert_eq!(&three, rd(&reader, three_pos, three_tag).as_ref());
        assert_eq!(&four, rd(&reader, four_pos, four_tag).as_ref());

        drop(reader);
        drop(writer);

        let (writer, reader) = wal(&file).unwrap();
        drop(writer);

        // Can create new mappings when writer is dropped
        assert_eq!(&one, rd(&reader, one_pos, one_tag).as_ref());
        assert_eq!(&two, rd(&reader, two_pos, two_tag).as_ref());
        assert_eq!(&three, rd(&reader, three_pos, three_tag).as_ref());
        assert_eq!(&four, rd(&reader, four_pos, four_tag).as_ref());

        // Verify that we can free unused mappings
        let (_, one_read) = reader.read(one_pos).unwrap();
        assert_eq!(reader.cleanup(), 1);
        assert_eq!(&one, one_read.as_ref());
        drop(one_read);
        assert_eq!(reader.cleanup(), 0);
        drop(reader);

        let (writer, reader) = wal(&file).unwrap();

        let mut iter = reader.iter_until(&writer);
        drop(writer);

        assert_eq!(&one, rd_it(&mut iter, one_tag, one_pos).as_ref());
        assert_eq!(&two, rd_it(&mut iter, two_tag, two_pos).as_ref());
        assert_eq!(&three, rd_it(&mut iter, three_tag, three_pos).as_ref());
        assert_eq!(&four, rd_it(&mut iter, four_tag, four_pos).as_ref());
        assert!(iter.next().is_none());
    }

    #[track_caller]
    // Read from position, assert tag
    fn rd(reader: &WalReader, pos: WalPosition, tag: Tag) -> Bytes {
        let (read_tag, data) = reader.read(pos).unwrap();
        assert_eq!(read_tag, tag);
        data
    }

    #[track_caller]
    // Read from iterator, assert tag and position
    fn rd_it(iter: &mut WalIterator, tag: Tag, pos: WalPosition) -> Bytes {
        let (read_pos, (read_tag, data)) = iter.next().unwrap();
        assert_eq!(read_tag, tag);
        assert_eq!(read_pos, pos);
        data
    }

    #[test]
    fn test_wal_read_after_write() {
        // Verify that mapping can read entries that are written to it after it is created
        let temp = tempdir::TempDir::new("test_wal").unwrap();
        let file = temp.path().join("wal");
        let (mut writer, reader) = wal(&file).unwrap();
        let one = [1u8; 15];
        let two = [2u8; 18];
        let one_pos = writer.write(5, &one).unwrap();
        let (one_tag, one_read) = reader.read(one_pos).unwrap();
        assert_eq!(&one, one_read.as_ref());
        assert_eq!(one_tag, 5);

        let two_pos = writer.write(6, &two).unwrap();
        let (two_tag, two_read) = reader.read(two_pos).unwrap();
        assert_eq!(&two, two_read.as_ref());
        assert_eq!(two_tag, 6);

        assert_eq!(1, reader.cleanup()); // assert only one mapping was created (therefore one and two share same mapping)
    }

    #[test]
    fn test_header_combine_split() {
        for crc in [0, 1, 12, u64::MAX] {
            for len in [0, 1, 18, u32::MAX as u64] {
                for tag in [0, 1, 18, u32::MAX] {
                    let combined = combine_header(crc, len, tag);
                    assert_eq!((crc, len, tag), split_header(combined));
                }
            }
        }
    }
}
