/*
 * Portions Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This software may be used and distributed according to the terms of the
 * GNU General Public License version 2.
 */

/* Copyright (c) 2018 Carl Lerche
 *
 * Permission is hereby granted, free of charge, to any
 * person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the
 * Software without restriction, including without
 * limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice
 * shall be included in all copies or substantial portions
 * of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
 * ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
 * SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#[macro_use]
extern crate bencher;

use bencher::Bencher;
use minibytes::Bytes;
use std::hint::black_box;

fn deref_unique(bench: &mut Bencher) {
    let buf = Bytes::from(vec![0; 1024]);
    bench.iter(|| {
        for _ in 0..1024 {
            black_box(&buf[..]);
        }
    });
}

fn deref_shared(bench: &mut Bencher) {
    let buf = Bytes::from(vec![0; 1024]);
    let _b2 = buf.clone();
    bench.iter(|| {
        for _ in 0..1024 {
            black_box(&buf[..]);
        }
    });
}

fn deref_static(bench: &mut Bencher) {
    let buf = Bytes::from_static(b"hello world");
    bench.iter(|| {
        for _ in 0..1024 {
            black_box(&buf[..]);
        }
    });
}

fn clone_static(bench: &mut Bencher) {
    let bytes =
        Bytes::from_static("hello world 1234567890 and have a good byte 0987654321".as_bytes());
    bench.iter(|| {
        for _ in 0..1024 {
            black_box(&bytes.clone());
        }
    });
}

fn clone_shared(bench: &mut Bencher) {
    let bytes = Bytes::from(b"hello world 1234567890 and have a good byte 0987654321".to_vec());
    bench.iter(|| {
        for _ in 0..1024 {
            black_box(&bytes.clone());
        }
    });
}

fn clone_arc_vec(bench: &mut Bencher) {
    use std::sync::Arc;
    let bytes = Arc::new(b"hello world 1234567890 and have a good byte 0987654321".to_vec());
    bench.iter(|| {
        for _ in 0..1024 {
            black_box(&bytes.clone());
        }
    });
}

fn from_long_slice(bench: &mut Bencher) {
    let data = [0u8; 128];
    bench.bytes = data.len() as u64;
    bench.iter(|| {
        let buf = Bytes::copy_from_slice(&data[..]);
        black_box(buf);
    });
}

fn slice_empty(bench: &mut Bencher) {
    bench.iter(|| {
        let b = Bytes::from(vec![17; 1024]).clone();
        for i in 0..1000 {
            black_box(b.slice(i % 100..i % 100));
        }
    });
}

fn slice_short_from_arc(bench: &mut Bencher) {
    bench.iter(|| {
        let b = Bytes::from(vec![17; 1024]).clone();
        for i in 0..1000 {
            black_box(b.slice(1..2 + i % 10));
        }
    });
}

benchmark_group!(
    benches,
    deref_unique,
    deref_shared,
    deref_static,
    clone_static,
    clone_shared,
    clone_arc_vec,
    from_long_slice,
    slice_empty,
    slice_short_from_arc
);
benchmark_main!(benches);
