use bytes::Bytes;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::slice;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

#[derive(Debug)]
pub struct Chunk {
    element_size: usize,  // 元素占用空间
    element_amount: usize, // 总元素个数
    is_not_full_element_exit: bool, // 是否有不全的元素
    data: Vec<MaybeUninit<u8>>,
    element_actual_size: Vec<usize>,
    element_actual_amount: usize,
}

impl Chunk {
    #[inline]
    fn new(element_amount: usize, element_size: usize) -> Self {
        if element_amount > usize::BITS as usize || element_amount == 0 {
            panic!("Unavailable chunk capacity: {}", element_amount);
        }
        let len = element_amount * element_size;
        let mut data = Vec::with_capacity(len);
        unsafe { data.set_len(len) };
        let mut element_actual_size = Vec::new();
        element_actual_size.resize(element_amount, 0);
        
        Chunk {
            data,
            element_amount,
            element_size,
            is_not_full_element_exit: false,
            element_actual_size,
            element_actual_amount: 0,
        }
    }

    #[inline]
    fn insert(&mut self, index: usize, value: impl Borrow<[u8]>) {
        // Index is the position of value in Chunk
        if index >= self.element_amount {
            panic!("Index out of bounds");
        }

        if value.borrow().len() > self.element_size {
            panic!("Wrong length");
        }
        
        if self.is_some(index) {
            panic!("Element added twice");
        }
        
        let value = value.borrow();
        unsafe {
            self.index_mut(index, value.len()).copy_from_slice(value);
        }
        
        if value.len() < self.element_size {
            self.is_not_full_element_exit = true;
        }
        
        self.element_actual_size[index] = value.len();
        self.element_actual_amount += 1;
    }

    #[inline]
    fn is_full(&self) -> bool {
        println!("{:?}",self);
        self.element_actual_amount == self.element_amount && !self.is_not_full_element_exit
    }

    #[inline]
    fn is_some(&self, index: usize) -> bool {
         index < self.element_amount && self.element_actual_size[index] > 0
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
        while i < self.element_amount {
            if self.is_some(i) {
                if self.element_actual_size[i] != self.element_size {
                    i += 1;
                    let slice = unsafe { self.slice(i-1..i).unwrap() };
                    result.push((i-1, Bytes::copy_from_slice(slice)));
                    continue;
                }
                let start = i;
                // 找到连续为1的区间
                i += 1;
                while self.is_some(i) {
                    i += 1;
                    if self.element_actual_size[i] != self.element_size {
                        break;
                    }
                }
                
                // 将该区间的元素复制到一个新的 Bytes 中
                let slice = unsafe { self.slice(start..i).unwrap() };
                result.push((start, Bytes::copy_from_slice(slice)));
            } else {
                i += 1;
            }
        }
        result
    }

    #[inline]
    unsafe fn index_mut(&mut self, index: usize, actual_size: usize) -> &mut [u8] {
        let start = index * self.element_size;
        unsafe {
            slice::from_raw_parts_mut(self.data.as_mut_ptr().add(start) as *mut u8, actual_size)
        }
    }

    // #[inline]
    // unsafe fn index(&self, index: usize) -> &[u8] {
    //     let start = index * self.element_size;
    //     unsafe { slice::from_raw_parts(self.data.as_ptr().add(start) as *const u8, self.element_size) }
    // }

    #[inline]
    unsafe fn slice(&self, index: Range<usize>) -> Option<&[u8]> {
        if index.start == index.end {
            return None;
        }
        let start = index.start * self.element_size;
        let end = index.end * self.element_size;
        
        let end_size = self.element_actual_size[index.end];
        let len = end - start - (self.element_size - end_size);
        unsafe { Some(slice::from_raw_parts(self.data.as_ptr().add(start) as *const u8, len)) }

    }
}

impl AsRef<Chunk> for Chunk {
    fn as_ref(&self) -> &Chunk {
        self
    }
}

#[derive(Debug)]
pub struct ChunkedBuffer {
    element_amount: usize, // 块固定的大小
    element_size: usize, // 每个块内元素的大小
    block_size: usize, // 每个块固定的大小
    // 使用 HashMap 保存各个块：key 为块编号，value 为块数据（使用 Option<T> 保存，便于记录未填充部分）
    blocks: HashMap<usize, Chunk>,
}

impl ChunkedBuffer {
    #[inline]
    pub fn new(element_amount: usize, element_size: usize) -> Self {
        Self {
            element_amount,
            element_size,
            block_size: element_amount * element_size,
            blocks: HashMap::new(),
        }
    }

    pub fn insert(&mut self, index: usize, item: impl Borrow<[u8]>) {
        // index is the position of single element in the whole Buffer
        let block_index = index / self.block_size;
        
        let remainder = index % self.block_size;
        if remainder % self.element_size != 0 {
            panic!("Remainder contribute to wrong memory struct");
        }
        let pos_in_block = remainder / self.element_size;

        if item.borrow().len() > self.element_size {
            panic!("Item out of bounds");
        }
        
        let block = self
            .blocks
            .entry(block_index)
            .or_insert(Chunk::new(self.element_amount, self.element_size));
        
        if block.is_some(pos_in_block) {
            panic!("Index already exists");
        }
        
        block.insert(pos_in_block, item);
    }

    // #[inline]
    // pub fn peek_first_chunk(&self, is_full: bool) -> Option<(usize, &Chunk)> {
    //     let mut iter = self.blocks.iter().peekable();
    //     loop {
    //         let (k, chunk) = iter.next()?;
    //         if is_full ^ chunk.is_full() {
    //             continue;
    //         } else {
    //             return Some((*k, chunk));
    //         }
    //     }
    // }

    #[inline]
    pub fn take_first_full_chunk(&mut self) -> Option<BufferEntryFull<Chunk>> {
        // 获取块编号最小的块的 key

        let mut keys = self.blocks.keys();
        loop {
            let block_index = *keys.next()?;
            if self.blocks[&block_index].is_full() {
                return Some(BufferEntryFull {
                    file_start: (self.block_size * block_index) as u64,
                    chunk: self.blocks.remove(&block_index)?,
                });
                
            }
            continue;
        }
    }

    #[inline]
    pub fn take_first_not_full_chunk(&mut self) -> Option<BufferEntryNotFull<Chunk>> {
        // 获取块编号最小的块的 key

        let mut keys = self.blocks.keys();
        loop {
            let block_index = *keys.next()?;
            if !self.blocks[&block_index].is_full() {
                return Some(BufferEntryNotFull {
                    file_start: (self.block_size * block_index) as u64,
                    element_size: self.element_size,
                    chunk: self.blocks.remove(&block_index)?,
                });
            }
            continue;
        }
    }
    
}

#[derive(Debug)]
pub struct BufferEntryFull<B: Borrow<Chunk>> {
    file_start: u64,
    chunk: B,
}

impl<B: Borrow<Chunk>> BufferEntryFull<B> {
    #[inline]
    pub async fn write_file(&self, file: &mut BufWriter<tokio::fs::File>) {
        file.seek(std::io::SeekFrom::Start(self.file_start))
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
    file_start: u64,
    element_size: usize,
    chunk: B,
}

impl<B: Borrow<Chunk>> BufferEntryNotFull<B> {
    #[inline]
    pub async fn write_file(&self, file: &mut BufWriter<tokio::fs::File>) {
        for (index, bytes) in self.chunk.borrow().non_full_bytes().iter() {
            file.seek(std::io::SeekFrom::Start(self.file_start + (*index * self.element_size) as u64))
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
        let mut buff = ChunkedBuffer::new(5,1);
        buff.insert(0, [0]);
        buff.insert(2, [1]);
        buff.insert(1, [1]);
        buff.insert(3, [2]);
        buff.insert(4, [3]);
        assert!(buff.take_first_full_chunk().is_some());
        
    }
}
