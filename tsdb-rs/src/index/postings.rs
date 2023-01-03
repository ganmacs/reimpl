use byteorder::{BigEndian, ReadBytesExt};
use std::collections::BinaryHeap;
use std::io;

pub enum Postings {
    Merged(MergedPostings),
    Empty(EmptyPostings),
    BigEndian(BigEndianPostings),

    // only for test
    List(ListPostings),
}

impl Postings {
    pub(crate) fn new_empty() -> Self {
        Postings::Empty(EmptyPostings)
    }

    pub(crate) fn new_merge(inner: Vec<Postings>) -> Self {
        Postings::Merged(MergedPostings::new(inner))
    }

    pub(crate) fn new_big_endian(inner: io::Cursor<Vec<u8>>) -> Self {
        Postings::BigEndian(BigEndianPostings::new(inner))
    }

    fn new_list(inner: Vec<u64>) -> Self {
        Postings::List(ListPostings::new(inner))
    }
}

impl Iterator for Postings {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        match self {
            Postings::Merged(inner) => inner.next(),
            Postings::BigEndian(inner) => inner.next(),
            Postings::Empty(inner) => inner.next(),
            Postings::List(inner) => inner.next(),
        }
    }
}

pub struct ErrorPostings {
    err: String,
}

pub struct EmptyPostings;

pub struct BigEndianPostings {
    inner: io::Cursor<Vec<u8>>,
}

pub struct MergedPostings {
    inner: Vec<Postings>,
    prev: Option<u64>,
    heap: BinaryHeap<(i64, usize)>,
}

impl MergedPostings {
    fn new(inner: Vec<Postings>) -> Self {
        let mut pos = MergedPostings {
            inner,
            prev: None,
            heap: BinaryHeap::new(),
        };

        for (idx, item) in pos.inner.iter_mut().enumerate() {
            if let Some(v) = item.next() {
                pos.heap.push((-(v as i64), idx));
            }
        }

        pos
    }
}

impl BigEndianPostings {
    pub(crate) fn new(inner: io::Cursor<Vec<u8>>) -> Self {
        Self { inner }
    }
}

impl Iterator for EmptyPostings {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        None
    }
}

impl Iterator for MergedPostings {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let v = match self.heap.pop() {
            Some((cur, idx)) => {
                if let Some(v) = self.inner[idx].next() {
                    self.heap.push((-(v as i64), idx));
                }
                Some(-cur as u64)
            }

            None => None,
        };

        if v == self.prev {
            self.next()
        } else {
            self.prev = v;
            v
        }
    }
}

impl Iterator for BigEndianPostings {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        self.inner.read_u32::<BigEndian>().map(|v| v as u64).ok()
    }
}

// only for test
pub struct ListPostings {
    cur: usize,
    inner: Vec<u64>,
}

impl ListPostings {
    fn new(inner: Vec<u64>) -> Self {
        ListPostings { cur: 0, inner }
    }
}

impl Iterator for ListPostings {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if self.inner.len() <= self.cur {
            None
        } else {
            let v = self.inner[self.cur];
            self.cur += 1;
            Some(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merged_postigns() {
        let pos1 = Postings::new_list(vec![1, 2, 3, 4, 5, 6, 7, 1000, 1001]);
        let pos2 = Postings::new_list(vec![2, 4, 5, 6, 7, 8, 999, 1001]);
        let pos3 = Postings::new_list(vec![1, 2, 5, 6, 7, 8, 1001, 1200]);

        let pos = MergedPostings::new(vec![pos1, pos2, pos3]).collect::<Vec<u64>>();

        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200], pos)
    }
}
