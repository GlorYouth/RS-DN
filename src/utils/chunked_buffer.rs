use bytes::Bytes;
use std::borrow::Borrow;

use std::mem::MaybeUninit;
use std::slice;

#[derive(Debug)]
pub struct Chunk {
    avail: usize,
    set_len: usize,
    capacity: usize,
    data: Vec<MaybeUninit<u8>>,
}

impl Chunk {
    #[inline]
    fn new(capacity: usize, set_len: usize) -> Self {
        if capacity > usize::BITS as usize || capacity == 0 {
            panic!("Unavailable chunk capacity: {}", capacity);
        }
        let len = capacity * set_len;
        let mut data = Vec::with_capacity(len);
        unsafe { data.set_len(len) };
        Chunk {
            avail: 0,
            data,
            capacity,
            set_len,
        }
    }

    #[inline]
    fn insert(&mut self, index: usize, value: impl Borrow<[u8]>) {
        if index >= self.capacity {
            panic!("Index out of bounds");
        }

        if value.borrow().len() != self.set_len {
            panic!("Wrong length");
        }

        if self.avail >> index & 1 == 0 {
            self.avail |= 1 << index;
        }
        let value = value.borrow();
        unsafe {
            self.index_mut(index).copy_from_slice(value);
        }
    }

    #[inline]
    fn is_full(&self) -> bool {
        let mask = if self.capacity == usize::BITS as usize {
            usize::MAX
        } else {
            (1 << self.capacity) - 1
        };
        (self.avail & mask) == mask
    }

    #[inline]
    fn is_some(&self, index: usize) -> bool {
        index < self.capacity && self.avail & (1 << index) != 0
    }

    #[inline]
    pub fn bytes(&self) -> Option<Bytes> {
        if !self.is_full() {
            return None;
        }
        let ptr = self.data.as_ptr() as *const u8;
        unsafe {
            let slice = slice::from_raw_parts(ptr, self.data.len());
            Some(Bytes::copy_from_slice(slice))
        }
    }

    #[inline]
    unsafe fn index_mut(&mut self, index: usize) -> &mut [u8] {
        let start = index * self.set_len;
        unsafe {
            slice::from_raw_parts_mut(self.data.as_mut_ptr().add(start) as *mut u8, self.set_len)
        }
    }

    #[inline]
    unsafe fn index(&self, index: usize) -> &[u8] {
        let start = index * self.set_len;
        unsafe { slice::from_raw_parts(self.data.as_ptr().add(start) as *const u8, self.set_len) }
    }
}

#[derive(Debug)]
pub struct ChunkedBuffer {
    block_size: usize, // 每个块固定的大小
    // 使用 BTreeMap 保存各个块：key 为块编号，value 为块数据（使用 Option<T> 保存，便于记录未填充部分）
    chunks: Vec<(usize, Chunk)>,
}

impl ChunkedBuffer {
    #[inline]
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            chunks: Vec::with_capacity(50),
        }
    }

    pub fn insert(&mut self, index: usize, item: impl Borrow<[u8]>) {
        let block_index = index / self.block_size;
        let pos_in_block = index % self.block_size;

        match self.chunks.iter_mut().find(|v| v.0 == block_index) {
            Some((_, chunk)) => {
                chunk.insert(pos_in_block, item);
            }
            None => self.chunks.push((
                pos_in_block,
                Chunk::new(self.block_size, item.borrow().len()),
            )),
        };
    }

    #[inline]
    pub fn peek_first_chunk(&self, is_full: bool) -> Option<&Chunk> {
        for (_, v) in self.chunks.iter().peekable() {
            if is_full ^ v.is_full() {
                continue;
            } else {
                return Some(v);
            }
        }
        None
    }

    pub fn take_first_chunk(&mut self, is_full: bool) -> Option<Chunk> {
        // 获取块编号最小的块的 key

        for (k, v) in self.chunks.iter().enumerate() {
            if is_full ^ v.1.is_full() {
                continue;
            } else {
                return Some(self.chunks.remove(k).1);
            }
        }
        None
    }

    #[inline]
    pub fn iter_full(&self) -> impl Iterator<Item = &(usize, Chunk)> {
        self.chunks.iter().filter(|(_, chunk)| chunk.is_full())
    }

    #[inline]
    pub fn iter_non_full(&self) -> impl Iterator<Item = &(usize, Chunk)> {
        self.chunks.iter().filter(|(_, chunk)| !chunk.is_full())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk() {
        let mut slice = [0_isize; 10];
        for i in 0..10 {
            slice[i] = i as isize;
        }
        let slice = unsafe { slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
        let mut chunk = Chunk::new(2, slice.len());
        let mut slice2 = [0_isize; 10];
        for i in 0..10 {
            slice2[i] = i as isize;
        }
        let slice2 =
            unsafe { slice::from_raw_parts(slice2.as_ptr() as *const u8, slice2.len() * 8) };
        chunk.insert(0, slice);
        chunk.insert(1, slice2);

        assert!(
            chunk
                .bytes()
                .unwrap()
                .iter()
                .eq(slice.iter().chain(slice2.iter()))
        );
    }
    #[test]
    fn test_chunk_buff() {
        let mut buff = ChunkedBuffer::new(5);
        buff.insert(0, [0]);
        assert_eq!(buff.peek_first_chunk(false).unwrap().avail, 1);
        unsafe {
            assert_eq!(*buff.peek_first_chunk(false).unwrap().index(0), [0]);
        }

        assert!(buff.peek_first_chunk(true).is_none());

        buff.insert(2, [1]);
        assert_eq!(buff.peek_first_chunk(false).unwrap().avail, 0b101);
        unsafe {
            assert_eq!(*buff.peek_first_chunk(false).unwrap().index(2), [1]);
        }

        assert!(buff.peek_first_chunk(true).is_none());

        buff.insert(1, [1]);
        assert_eq!(buff.peek_first_chunk(false).unwrap().avail, 0b111);

        buff.insert(3, [2]);
        buff.insert(4, [3]);

        assert_eq!(buff.peek_first_chunk(false).is_some(), false);
        assert_eq!(buff.peek_first_chunk(true).is_some(), true);
        println!("{:?}", buff.take_first_chunk(true));
    }
}
