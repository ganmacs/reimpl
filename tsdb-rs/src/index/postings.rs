use byteorder::{BigEndian, ReadBytesExt};
use std::collections::BinaryHeap;
use std::io;

pub struct ErrorPostings {
    err: String,
}

pub struct EmptyPostings;

pub struct BigEndianPostings {
    inner: io::Cursor<Vec<u8>>,
}

pub struct MergedPostings<T> {
    inner: Vec<T>,
    prev: Option<u64>,
    heap: BinaryHeap<(i64, usize)>,
}

impl<T: Postings> MergedPostings<T> {
    fn new(inner: Vec<T>) -> Self {
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

impl<T: Postings> Iterator for MergedPostings<T> {
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

pub trait Postings: Iterator<Item = u64> {}
impl<T: Postings> Postings for MergedPostings<T> {}
impl Postings for EmptyPostings {}
impl Postings for BigEndianPostings {}

#[cfg(test)]
mod tests {
    use super::*;

    struct ListPostings {
        cur: usize,
        list: Vec<u64>,
    }

    impl ListPostings {
        fn new(list: Vec<u64>) -> Self {
            ListPostings { cur: 0, list }
        }
    }

    impl Iterator for ListPostings {
        type Item = u64;

        fn next(&mut self) -> Option<u64> {
            if self.list.len() <= self.cur {
                None
            } else {
                let v = self.list[self.cur];
                self.cur += 1;
                Some(v)
            }
        }
    }
    impl Postings for ListPostings {}

    #[test]
    fn test_merged_postigns() {
        let pos1 = ListPostings::new(vec![1, 2, 3, 4, 5, 6, 7, 1000, 1001]);
        let pos2 = ListPostings::new(vec![2, 4, 5, 6, 7, 8, 999, 1001]);
        let pos3 = ListPostings::new(vec![1, 2, 5, 6, 7, 8, 1001, 1200]);

        let pos = MergedPostings::new(vec![pos1, pos2, pos3]).collect::<Vec<u64>>();

        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200], pos)
    }
}
