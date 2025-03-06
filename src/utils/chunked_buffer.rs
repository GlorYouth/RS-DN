use bytes::Bytes;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ops::Range;
use std::slice;
use std::sync::Arc;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;

#[derive(Debug)]
pub struct BlockInfo {
    element_size: usize,   // 元素占用空间
    element_amount: usize, // 每个块的元素个数

    block_size: usize, // 每个块固定的大小
    total_size: usize, // 总文件大小
}

impl BlockInfo {
    #[inline]
    pub const fn new(element_size: usize, element_amount: usize, total_size: usize) -> Self {
        Self {
            element_size,
            element_amount,
            block_size: element_size * element_amount,
            total_size,
        }
    }

    #[inline]
    fn get_last_element_size(&self) -> usize {
        match self.total_size % self.element_size {
            0 => self.element_size,
            size => size,
        }
    }

    #[inline]
    fn get_last_block_index(&self) -> usize {
        (self.total_size - 1) / self.block_size
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
    fn insert(&mut self, pos_in_block: usize, bytes: impl Borrow<[u8]>) {
        if pos_in_block >= self.info.element_amount {
            panic!("Index out of bounds");
        }

        let bytes = bytes.borrow();

        if bytes.len() > self.info.element_size {
            panic!("Wrong length");
        }

        if self.is_some(pos_in_block) {
            panic!("Element added twice");
        }

        if let Some(size) = self.last_element_size {
            if size != self.info.element_size {
                self.is_not_full_element_exist = true;
            } else {
            }
        } else if bytes.len() < self.info.element_size {
            panic!("Element size too small");
        }

        self.element_copy_from_slice(bytes, pos_in_block);

        self.avail |= 1 << pos_in_block;
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
    fn element_copy_from_slice(&mut self, slice: &[u8], pos_in_block: usize) {
        if slice.len() > self.info.element_size {
            panic!("Element size too big");
        }
        let start = pos_in_block * self.info.element_size;
        unsafe {
            slice::from_raw_parts_mut(
                self.elements.as_mut_ptr().add(start) as *mut u8,
                slice.len(),
            )
            .copy_from_slice(slice);
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
            Some(last_element_size) => last_element_size,
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
        // start is the position of the first byte in the whole buffer
        let block_index = start / self.info.block_size;

        let remainder = start % self.info.block_size;
        if remainder % self.info.element_size != 0 {
            panic!("Remainder contribute to wrong memory struct");
        }
        let pos_in_block = remainder / self.info.element_size;

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
    pub async fn write_file(&self, file: &mut tokio::fs::File) {
        file.seek(std::io::SeekFrom::Start(self.file_start))
            .await
            .expect("Failed to seek");
        let bytes = self.chunk.borrow().full_bytes().unwrap();
        file.write_all(bytes.as_ref())
            .await
            .expect("Failed to write chunk");
        file.flush().await.expect("Failed to flush chunk");
    }
}

pub struct BufferEntryNotFull<B: Borrow<Chunk>> {
    file_start: u64,
    element_size: usize,
    chunk: B,
}

impl<B: Borrow<Chunk>> BufferEntryNotFull<B> {
    #[inline]
    pub async fn write_file(&self, file: &mut tokio::fs::File) {
        for (index, bytes) in self.chunk.borrow().non_full_bytes().iter() {
            file.seek(std::io::SeekFrom::Start(
                self.file_start + (*index * self.element_size) as u64,
            ))
            .await
            .expect("Failed to seek");
            file.write_all(bytes.as_ref())
                .await
                .expect("Failed to write chunk");
            file.flush().await.expect("Failed to flush chunk");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_chunk_full_bytes() {
        let info = Arc::new(BlockInfo::new(16, 16, 16 * 16));
        let mut chunk = Chunk::new(info.clone(), None);
        let mut vec = Vec::with_capacity(info.element_amount);
        for i in 0..info.element_amount {
            let mut in_vec = Vec::with_capacity(info.element_size);
            for _ in 0..info.element_size {
                in_vec.push(rand::random::<u8>());
            }
            chunk.insert(i, in_vec.clone());
            vec.push(in_vec);
        }
        let bytes = chunk.full_bytes().unwrap();
        for i in 0..info.element_amount {
            for j in 0..info.element_size {
                assert_eq!(bytes[i * info.element_size + j], vec[i][j]);
            }
        }
    }
    #[tokio::test]
    async fn test_chunk_buff() {
        let info = Arc::new(BlockInfo::new(16, 16, 16 * 16));
        let mut buffer = ChunkedBuffer::new(info.clone());
        let mut vec = Vec::with_capacity(info.element_amount);
        for i in 0..info.element_amount {
            let mut in_vec = Vec::with_capacity(info.element_size);
            for _ in 0..info.element_size {
                in_vec.push(rand::random::<u8>());
            }
            buffer.insert(i * info.element_size, in_vec.clone());
            vec.push(in_vec);
        }
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open("check_buff")
            .await
            .expect("Failed to open output file");

        buffer
            .take_first_full_chunk()
            .unwrap()
            .write_file(&mut file)
            .await;
        let file = std::fs::File::open("check_buff").expect("Failed to open output file");
        let mut reader = std::io::BufReader::new(file);
        let mut buff = [0; 16 * 16];
        let _ = reader.read(&mut buff).expect("Failed to read from file");
        for i in 0..info.element_amount {
            for j in 0..info.element_size {
                assert_eq!(buff[i * info.element_size + j], vec[i][j]);
            }
        }
    }
}
