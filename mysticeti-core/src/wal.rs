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
use std::io::{IoSlice, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::Arc;

pub struct WalWriter {
    file: File,
    pos: u64,
}

// todo(andrey)
// - iteration
#[derive(Clone)]
pub struct WalReader {
    fd: RawFd,
    maps: Arc<Mutex<BTreeMap<u64, Bytes>>>,
}

#[derive(Clone, Copy)]
pub struct WalPosition {
    start: u64,
    len: u64,
}

pub fn wal(path: impl AsRef<Path>) -> io::Result<(WalWriter, WalReader)> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
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
const CRC_LEN_BYTES: u64 = 8;

const fn assert_constants() {
    if u64::MAX - MAP_MASK != MAP_SIZE - 1 {
        panic!("MAP_MASK and MAP_SIZE do not match");
    }
}

fn align_map_size(p: u64) -> u64 {
    p & MAP_MASK
}

impl WalWriter {
    pub fn write(&mut self, b: &[u8]) -> io::Result<WalPosition> {
        let len = b.len() as u64 + CRC_LEN_BYTES;
        assert!(len <= MAP_SIZE);
        let mut buffs = vec![];
        let mut written_expected = 0usize;
        eprintln!(
            "pos={}, len={}, self.pos + len - 1={}, a(pos)={}, a(pos+len)={}",
            self.pos,
            len,
            self.pos + len - 1,
            align_map_size(self.pos),
            align_map_size(self.pos + len - 1)
        );
        if align_map_size(self.pos) != align_map_size(self.pos + len - 1) {
            let extra_len = align_map_size(self.pos + len - 1) - self.pos;
            let extra = &ZERO_MAP[0..(extra_len as usize)];
            eprintln!("extra_len={extra_len}");
            buffs.push(IoSlice::new(extra));
            written_expected += extra.len();
            self.pos += extra_len;
            debug_assert_eq!(align_map_size(self.pos), self.pos);
            debug_assert_eq!(align_map_size(self.pos), align_map_size(self.pos + len - 1));
        }
        let crc = CRC.checksum(b).to_le_bytes();
        buffs.push(IoSlice::new(&crc));
        buffs.push(IoSlice::new(b));
        written_expected += len as usize;
        let written = self.file.write_vectored(&buffs)?;
        assert_eq!(written, written_expected);
        let position = WalPosition {
            start: self.pos,
            len,
        };
        self.pos += len;
        Ok(position)
    }
}

impl WalReader {
    pub fn read(&self, position: WalPosition) -> io::Result<Bytes> {
        assert!(position.len as u64 <= MAP_SIZE);
        let mut maps = self.maps.lock();
        let offset = align_map_size(position.start);
        println!("offset={offset}");
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
        let buf_offset = (position.start - offset) as usize;
        let mut crc = [0u8; 8];
        crc.copy_from_slice(&bytes[buf_offset..buf_offset + (CRC_LEN_BYTES as usize)]);
        let expected_crc = u64::from_le_bytes(crc);
        let bytes = bytes
            .slice(buf_offset + (CRC_LEN_BYTES as usize)..buf_offset + (position.len as usize));
        let actual_crc = CRC.checksum(bytes.as_ref());
        if actual_crc != expected_crc {
            // todo - return error
            panic!(
                "Crc mismatch, expected {}, found {} at position {}:{}",
                expected_crc, actual_crc, position.start, position.len
            );
        }
        Ok(bytes)
    }

    // Attempts cleaning internal mem maps, returning number of retained maps
    // Map can be freed when all buffers linked to this portion of a file are dropped
    pub fn cleanup(&self) -> usize {
        let mut maps = self.maps.lock();
        maps.retain(|_k, v| v.downcast_mut::<Mmap>().is_none());
        maps.len()
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
        let two = [2u8; (MAP_SIZE - CRC_LEN_BYTES) as usize];
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
}
