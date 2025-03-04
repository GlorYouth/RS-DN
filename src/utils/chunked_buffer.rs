use bytes::Bytes;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::slice;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

#[derive(Debug)]
pub struct Chunk {
    avail: usize,    // 成功初始化的标记
    set_len: usize,  // 输入数组的固定长度
    capacity: usize, // 总元素个数
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
        // Index is the position of value in Chunk
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
    pub fn full_bytes(&self) -> Option<Bytes> {
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
    pub fn non_full_bytes(&self) -> Vec<(usize, Bytes)> {
        // 当前内部索引和连续块
        let mut result = Vec::new();
        let mut i = 0;
        while i < self.capacity {
            if (self.avail >> i) & 1 == 1 {
                let start = i;
                // 找到连续为1的区间
                while i < self.capacity && ((self.avail >> i) & 1 == 1) {
                    i += 1;
                }
                // 将该区间的元素复制到一个新的 Bytes 中
                let slice = unsafe { self.slice(start..i) };
                result.push((start, Bytes::copy_from_slice(slice)));
            } else {
                i += 1;
            }
        }
        result
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

    #[inline]
    unsafe fn slice(&self, index: Range<usize>) -> &[u8] {
        let start = index.start * self.set_len;
        let end = index.end * self.set_len;
        unsafe { slice::from_raw_parts(self.data.as_ptr().add(start) as *const u8, end - start) }
    }
}

impl AsRef<Chunk> for Chunk {
    fn as_ref(&self) -> &Chunk {
        self
    }
}

#[derive(Debug)]
pub struct ChunkedBuffer {
    block_size: usize, // 每个块固定的大小
    // 使用 HashMap 保存各个块：key 为块编号，value 为块数据（使用 Option<T> 保存，便于记录未填充部分）
    chunks: HashMap<usize, Chunk>,
}

impl ChunkedBuffer {
    #[inline]
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            chunks: HashMap::new(),
        }
    }

    pub fn insert(&mut self, index: usize, item: impl Borrow<[u8]>) {
        // index is the position of single element in the whole Buffer
        let block_index = index / self.block_size;
        let pos_in_block = index % self.block_size;

        let block = self
            .chunks
            .entry(block_index)
            .or_insert(Chunk::new(self.block_size, item.borrow().len()));
        if block.is_some(pos_in_block) {
            panic!("Index already exists");
        }
        block.insert(pos_in_block, item);
    }

    #[inline]
    pub fn peek_first_chunk(&self, is_full: bool) -> Option<(usize, &Chunk)> {
        let mut iter = self.chunks.iter().peekable();
        loop {
            let (k, chunk) = iter.next()?;
            if is_full ^ chunk.is_full() {
                continue;
            } else {
                return Some((*k, chunk));
            }
        }
    }

    #[inline]
    pub fn take_first_full_chunk(&mut self) -> Option<BufferEntryFull<Chunk>> {
        // 获取块编号最小的块的 key

        let mut keys = self.chunks.keys();
        loop {
            let k = *keys.next()?;
            if self.chunks[&k].is_full() {
                continue;
            } else {
                return Some(BufferEntryFull {
                    block_index: k,
                    block_size: self.block_size,
                    chunk: self.chunks.remove(&k)?,
                });
            }
        }
    }

    #[inline]
    pub fn take_first_not_full_chunk(&mut self) -> Option<BufferEntryFull<Chunk>> {
        // 获取块编号最小的块的 key

        let mut keys = self.chunks.keys();
        loop {
            let k = *keys.next()?;
            if !self.chunks[&k].is_full() {
                continue;
            } else {
                return Some(BufferEntryFull {
                    block_index: k,
                    block_size: self.block_size,
                    chunk: self.chunks.remove(&k)?,
                });
            }
        }
    }
    
}

#[derive(Debug)]
pub struct BufferEntryFull<B: Borrow<Chunk>> {
    block_index: usize,
    block_size: usize,
    chunk: B,
}

impl<B: Borrow<Chunk>> BufferEntryFull<B> {
    #[inline]
    pub async fn write_file(&self, file: &mut BufWriter<tokio::fs::File>) {
        file.seek(std::io::SeekFrom::Start(
            (self.block_index * self.block_size) as u64,
        ))
        .await
        .expect("Failed to seek");
        file.write_all(
            self.chunk
                .borrow()
                .full_bytes()
                .expect("Failed to get bytes")
                .as_ref(),
        )
        .await
        .expect("Failed to write chunk");
    }
}

pub struct BufferEntryNotFull<B: Borrow<Chunk>> {
    block_index: usize,
    block_size: usize,
    chunk: B,
}

impl<B: Borrow<Chunk>> BufferEntryNotFull<B> {
    #[inline]
    pub async fn write_file(&self, file: &mut BufWriter<tokio::fs::File>) {
        let start = self.block_index * self.block_size;
        for (index, bytes) in self.chunk.borrow().non_full_bytes().iter() {
            file.seek(std::io::SeekFrom::Start((start + *index) as u64))
                .await
                .expect("Failed to seek");
            file.write_all(bytes.as_ref())
                .await
                .expect("Failed to write chunk");
        }
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
                .full_bytes()
                .unwrap()
                .iter()
                .eq(slice.iter().chain(slice2.iter()))
        );
    }
    #[test]
    fn test_chunk_buff() {
        let mut buff = ChunkedBuffer::new(5);
        buff.insert(0, [0]);
        assert_eq!(buff.peek_first_chunk(false).unwrap().1.avail, 1);
        unsafe {
            assert_eq!(*buff.peek_first_chunk(false).unwrap().1.index(0), [0]);
        }

        assert!(buff.peek_first_chunk(true).is_none());

        buff.insert(2, [1]);
        assert_eq!(buff.peek_first_chunk(false).unwrap().1.avail, 0b101);
        unsafe {
            assert_eq!(*buff.peek_first_chunk(false).unwrap().1.index(2), [1]);
        }

        assert!(buff.peek_first_chunk(true).is_none());

        buff.insert(1, [1]);
        assert_eq!(buff.peek_first_chunk(false).unwrap().1.avail, 0b111);

        buff.insert(3, [2]);
        buff.insert(4, [3]);

        assert_eq!(buff.peek_first_chunk(false).is_some(), false);
        assert_eq!(buff.peek_first_chunk(true).is_some(), true);
        println!("{:?}", buff.take_first_full_chunk());
    }
}
