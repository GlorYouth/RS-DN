use bytes::Bytes;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::slice;
use std::sync::Arc;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};

#[derive(Debug)]
pub struct BlockInfo {
    element_size: usize,   // 元素占用空间
    element_amount: usize, // 总元素个数

    block_size: usize, // 每个块固定的大小
    total_size: usize,
}

impl BlockInfo {
    #[inline]
    pub fn new(element_size: usize, element_amount: usize, total_size: usize) -> Self {
        Self {
            element_size,
            element_amount,
            block_size: element_size * element_amount,
            total_size,
        }
    }
    
    #[inline]
    fn get_last_element_size(&self) -> usize {
        (self.total_size - 1) % self.element_size
    }
    
    #[inline]
    fn get_last_block_index(&self) -> usize {
        (self.total_size) / self.block_size
    }
}


#[derive(Debug)]
pub struct Chunk {
    info: Arc<BlockInfo>,
    is_not_full_element_exist: bool, // 是否存在未满的元素
    last_element_size: Option<usize>,
    elements: Vec<MaybeUninit<u8>>,
    avail: usize, // 对应元素是否初始化的标记
}

impl Chunk {
    #[inline]
    fn new(info: Arc<BlockInfo>, last_element_size: Option<usize>) -> Self {
        if info.element_amount > usize::BITS as usize || info.element_amount == 0 {
            panic!("Unavailable chunk capacity: {}", info.element_amount);
        }
        let len = info.element_amount * info.element_size;
        let mut data = Vec::with_capacity(len);
        unsafe { data.set_len(len) };

        Chunk {
            elements: data,
            info,
            is_not_full_element_exist: false,
            last_element_size,
            avail: 0,
        }
    }

    #[inline]
    fn insert(&mut self, index: usize, value: impl Borrow<[u8]>) {
        // Index is the position of value in Chunk
        if index >= self.info.element_amount {
            panic!("Index out of bounds");
        }

        let value = value.borrow();

        if value.len() > self.info.element_size {
            panic!("Wrong length");
        }

        if self.is_some(index) {
            panic!("Element added twice");
        }

        if let Some(size) = self.last_element_size {
            if size != self.info.element_size {
                self.is_not_full_element_exist = true;
            } else {}
        } else if value.len() < self.info.element_size {
            panic!("Element size too small");
        }
        
        unsafe {
            self.index_mut(index, value.len()).copy_from_slice(value);
        }
        self.avail |= 1 << index;
    }

    #[inline]
    fn is_full(&self) -> bool {
        if !self.is_not_full_element_exist {
            if self.info.element_amount == usize::BITS as usize && !self.avail == 0 {
                true
            } else if (self.avail + 1) >> self.info.element_amount == 1 {
                true
            } else { 
                false
            }
        } else { 
            false
        }
    }

    #[inline]
    fn is_some(&self, index: usize) -> bool {
        index < self.info.element_amount && self.avail >> index & 1 == 1
    }

    #[inline]
    pub fn full_bytes(&self) -> Option<Bytes> {
        if !self.is_full() {
            return None;
        }
        let ptr = self.elements.as_ptr() as *const u8;
        unsafe {
            let slice = slice::from_raw_parts(ptr, self.elements.len());
            Some(Bytes::copy_from_slice(slice))
        }
    }

    #[inline]
    pub fn non_full_bytes(&self) -> Vec<(usize, Bytes)> {
        // 当前内部索引和连续块
        let mut result = Vec::new();
        let mut i = 0;
        while i < self.info.element_amount {
            if self.is_some(i) {
                let start = i;
                // 找到连续为1的区间
                i += 1;
                while self.is_some(i) {
                    i += 1;
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
        let start = index * self.info.element_size;
        unsafe {
            slice::from_raw_parts_mut(
                self.elements.as_mut_ptr().add(start) as *mut u8,
                actual_size,
            )
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
        let start = index.start * self.info.element_size;
        let end = index.end * self.info.element_size;

        let end_size = match self.last_element_size {
            Some(last_element_size) => {
                last_element_size
            }
            None => self.info.element_size,
        };
        let len = end - start - (self.info.element_size - end_size);
        unsafe {
            Some(slice::from_raw_parts(
                self.elements.as_ptr().add(start) as *const u8,
                len,
            ))
        }
    }
}

impl AsRef<Chunk> for Chunk {
    fn as_ref(&self) -> &Chunk {
        self
    }
}

#[derive(Debug)]
pub struct ChunkedBuffer {
    info: Arc<BlockInfo>,
    // 使用 HashMap 保存各个块：key 为块编号，value 为块数据（使用 Option<T> 保存，便于记录未填充部分）
    blocks: HashMap<usize, Chunk>,
}

impl ChunkedBuffer {
    #[inline]
    pub fn new(info: Arc<BlockInfo>) -> Self {
        Self {
            info,
            blocks: HashMap::new(),
        }
    }

    pub fn insert(&mut self, start: usize, bytes: impl Borrow<[u8]>) {
        // index is the position of single element in the whole Buffer
        let block_index = start / self.info.block_size;

        let remainder = start % self.info.block_size;
        if remainder % self.info.element_amount != 0 {
            panic!("Remainder contribute to wrong memory struct");
        }
        let pos_in_block = remainder / self.info.element_amount;

        if bytes.borrow().len() > self.info.element_size {
            panic!("Item out of bounds");
        }
        
        let last_element_size = if block_index == self.info.get_last_block_index() {
            Some(self.info.get_last_element_size())
        } else { 
            None
        };

        let block = self
            .blocks
            .entry(block_index)
            .or_insert(Chunk::new(self.info.clone(), last_element_size));

        if block.is_some(pos_in_block) {
            panic!("Index already exists");
        }

        block.insert(pos_in_block, bytes);
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
                    file_start: (self.info.block_size * block_index) as u64,
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
                    file_start: (self.info.block_size * block_index) as u64,
                    element_size: self.info.element_size,
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
            file.seek(std::io::SeekFrom::Start(
                self.file_start + (*index * self.element_size) as u64,
            ))
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
        let mut chunk = Chunk::new(Arc::new({
            BlockInfo {
                element_size: size_of::<usize>() * 10,
                element_amount: 2,
                block_size: 2 * (size_of::<usize>() * 10),
                total_size: 2 * (size_of::<usize>() * 10),
            }
        }), None);
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
        let mut buff = ChunkedBuffer::new(Arc::new(
            BlockInfo {
                element_size: 1,
                element_amount: 5,
                block_size: 5,
                total_size: 5,
            }
        ));
        buff.insert(0, [0]);
        buff.insert(2, [1]);
        buff.insert(1, [1]);
        buff.insert(3, [2]);
        buff.insert(4, [3]);
        assert!(buff.take_first_full_chunk().is_some());
    }
}
