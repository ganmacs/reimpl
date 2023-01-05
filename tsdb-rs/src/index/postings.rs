use byteorder::{BigEndian, ByteOrder};
use std::collections::BinaryHeap;

#[derive(Clone)]
pub enum Postings {
    Merged(MergedPostings),
    Empty(EmptyPostings),
    BigEndian(BigEndianPostings),
    Intersect(IntersectPostings),

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

    pub(crate) fn new_big_endian(inner: Vec<u8>) -> Self {
        Postings::BigEndian(BigEndianPostings::new(inner))
    }

    fn new_list(inner: Vec<u64>) -> Self {
        Postings::List(ListPostings::new(inner))
    }

    fn seek(&mut self, x: u64) {
        match self {
            Postings::Merged(inner) => todo!(),
            Postings::BigEndian(inner) => inner.seek(x),
            Postings::Empty(inner) => todo!(),
            Postings::List(inner) => inner.seek(x),
            Postings::Intersect(inner) => todo!(),
        }
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
            Postings::Intersect(inner) => inner.next(),
        }
    }
}

#[derive(Clone)]
pub struct ErrorPostings {
    err: String,
}

#[derive(Clone)]
pub struct EmptyPostings;

#[derive(Clone)]
pub struct BigEndianPostings {
    cur: usize,
    inner: Vec<u8>,
    prev: Option<u64>,
}

#[derive(Clone)]
pub struct IntersectPostings {
    inner: Vec<Postings>,
    prevs: Vec<Option<u64>>,
}

#[derive(Clone)]
pub struct MergedPostings {
    inner: Vec<Postings>,
    prev: Option<u64>,
    heap: BinaryHeap<(i64, usize)>,
}

impl BigEndianPostings {
    pub(crate) fn new(inner: Vec<u8>) -> Self {
        Self {
            cur: 0,
            inner,
            prev: None,
        }
    }

    pub(crate) fn seek(&mut self, x: u64) {
        match self.prev {
            Some(c) if c >= x => return,
            _ => {}
        };

        let offsets = (self.cur..self.inner.len())
            .step_by(4)
            .collect::<Vec<usize>>();

        let i = match offsets
            .binary_search_by(|off| BigEndian::read_u32(&self.inner[*off..]).cmp(&(x as u32)))
        {
            Ok(i) => i,
            Err(i) => i,
        };

        if i >= offsets.len() {
            self.cur = offsets[offsets.len() - 1] + 4;
        } else {
            self.cur = offsets[i].max(i);
        }
    }
}

impl IntersectPostings {
    fn new(inner: Vec<Postings>) -> Self {
        let l = &inner.len();

        Self {
            inner,
            prevs: vec![None; *l],
        }
    }
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

    pub(crate) fn seek(&mut self, x: u64) {
        self.prev = None;
        let mut tmp = vec![];

        while let Some((cur, idx)) = self.heap.pop() {
            if (-cur) < (x as i64) {
                tmp.push(idx);
            }
        }

        for idx in tmp {
            self.inner[idx].seek(x);

            if let Some(v) = self.inner[idx].next() {
                self.heap.push((-(v as i64), idx));
            }
        }
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

        if v.is_none() {
            return None;
        }

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
        if self.inner.len() >= (self.cur + 4) {
            let r = BigEndian::read_u32(&self.inner[self.cur..]) as u64;
            self.cur += 4;
            self.prev = Some(r);

            self.prev
        } else {
            None
        }
    }
}

impl Iterator for IntersectPostings {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if self.inner.len() == 0 {
            None
        } else if self.inner.len() == 1 {
            self.inner[0].next()
        } else {
            while let Some(inn) = self.inner[0].next() {
                let mut ok = true;

                for i in 1..self.inner.len() {
                    if let Some(v) = self.prevs[i] {
                        if v == inn {
                            continue;
                        }

                        if v > inn {
                            ok = false;
                            continue;
                        }
                    }

                    self.inner[i].seek(inn);
                    if let Some(c) = self.inner[i].next() {
                        self.prevs[i] = Some(c);

                        if c == inn {
                            continue;
                        }
                    }

                    ok = false;
                }

                if ok {
                    return Some(inn);
                }
            }

            None
        }
    }
}

// only for test
#[derive(Clone)]
pub struct ListPostings {
    cur: usize,
    inner: Vec<u64>,
}

impl ListPostings {
    fn new(inner: Vec<u64>) -> Self {
        ListPostings { cur: 0, inner }
    }

    pub fn seek(&mut self, x: u64) {
        if self.cur >= (x as usize) {
            return;
        }

        if self.inner.len() == 0 {
            return;
        }

        let i = match self.inner.binary_search_by(|inn| inn.cmp(&x)) {
            Ok(i) => i,
            Err(i) => i,
        };

        self.cur = self.cur.max(i);
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
    use byteorder::WriteBytesExt;

    use super::*;

    #[test]
    fn test_listing_seek() {
        let mut pos1 = Postings::new_list(vec![1, 2, 3, 4, 5, 6, 7, 1000, 1001]);

        pos1.seek(3);
        assert_eq!(
            vec![3, 4, 5, 6, 7, 1000, 1001],
            pos1.clone().collect::<Vec<u64>>()
        );

        pos1.seek(8);
        assert_eq!(vec![1000, 1001], pos1.clone().collect::<Vec<u64>>());

        pos1.seek(1002);
        assert_eq!(vec![] as Vec<u64>, pos1.clone().collect::<Vec<u64>>());

        pos1.seek(3);
        assert_eq!(vec![] as Vec<u64>, pos1.clone().collect::<Vec<u64>>());
    }

    #[test]
    fn test_merged_postigns() {
        let pos1 = Postings::new_list(vec![1, 2, 3, 4, 5, 6, 7, 1000, 1001]);
        let pos2 = Postings::new_list(vec![2, 4, 5, 6, 7, 8, 999, 1001]);
        let pos3 = Postings::new_list(vec![1, 2, 5, 6, 7, 8, 1001, 1200]);

        let mut merged = MergedPostings::new(vec![pos1, pos2, pos3]);

        assert_eq!(
            vec![1, 2, 3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200],
            merged.clone().collect::<Vec<u64>>()
        );

        merged.seek(3);
        assert_eq!(
            vec![3, 4, 5, 6, 7, 8, 999, 1000, 1001, 1200],
            merged.clone().collect::<Vec<u64>>()
        );

        merged.seek(9);
        assert_eq!(
            vec![999, 1000, 1001, 1200],
            merged.clone().collect::<Vec<u64>>()
        );

        merged.seek(1201);
        assert_eq!(vec![] as Vec<u64>, merged.clone().collect::<Vec<u64>>());
    }

    #[test]
    fn test_big_endian_postings() {
        let mut v = vec![];
        v.write_u32::<BigEndian>(1).unwrap();
        v.write_u32::<BigEndian>(2).unwrap();
        v.write_u32::<BigEndian>(3).unwrap();
        v.write_u32::<BigEndian>(4).unwrap();
        v.write_u32::<BigEndian>(6).unwrap();

        let mut pos = Postings::new_big_endian(v);
        assert_eq!(vec![1, 2, 3, 4, 6], pos.clone().collect::<Vec<u64>>());

        pos.seek(1);
        assert_eq!(vec![1, 2, 3, 4, 6], pos.clone().collect::<Vec<u64>>());

        pos.seek(3);
        assert_eq!(vec![3, 4, 6], pos.clone().collect::<Vec<u64>>());

        pos.seek(1);
        assert_eq!(vec![3, 4, 6], pos.clone().collect::<Vec<u64>>());

        pos.seek(5);
        assert_eq!(vec![6], pos.clone().collect::<Vec<u64>>());

        pos.seek(7);
        assert_eq!(vec![] as Vec<u64>, pos.clone().collect::<Vec<u64>>());
    }

    #[test]
    fn test_intersect_postings() {
        let pos1 = Postings::new_list(vec![1, 2, 3, 4, 5]);
        let pos2 = Postings::new_list(vec![4, 5, 6, 7, 8, 100]);

        let mut intersect = IntersectPostings::new(vec![pos1, pos2]);
        assert_eq!(vec![4, 5], intersect.clone().collect::<Vec<u64>>());
    }
}
