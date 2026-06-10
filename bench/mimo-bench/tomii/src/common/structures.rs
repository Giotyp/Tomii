#![allow(dead_code)]

use libc::{mmap, MAP_ANONYMOUS, MAP_FAILED, MAP_FIXED, MAP_PRIVATE, PROT_READ, PROT_WRITE};
use std::{
    mem,
    ops::{Index, IndexMut},
};

use libc;

const ALIGNMENT: usize = 64;

pub trait GenType: Clone + Default {}
impl<T: Clone + Default> GenType for T {}

#[repr(C, align(64))]
pub struct Table<T: GenType> {
    n_rows: usize,
    n_cols: usize,
    buffer: AlignedVec<T>, // contiguous backing storage of size n_rows * n_cols
}

impl<T: GenType> Table<T> {
    // Create Table array with dimensions [n_rows, n_cols]
    pub fn new(n_rows: usize, n_cols: usize) -> Self {
        let total = n_rows * n_cols;
        let buffer = AlignedVec::new(total, ALIGNMENT);
        Table {
            n_rows,
            n_cols,
            buffer,
        }
    }

    pub fn rows(&self) -> usize {
        self.n_rows
    }
    pub fn cols(&self) -> usize {
        self.n_cols
    }

    pub fn get(&self, row: usize) -> &[T] {
        let start = row * self.n_cols;
        let end = start + self.n_cols;
        &self.buffer.get()[start..end]
    }

    pub fn get_mut(&mut self, row: usize) -> &mut [T] {
        let start = row * self.n_cols;
        let end = start + self.n_cols;
        &mut self.buffer.get_mut()[start..end]
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // return the full contiguous backing storage
        self.buffer.get_mut()
    }
}

// Table row view removed; get/get_mut return slices directly

#[repr(C, align(64))]
pub struct Grid<T: GenType> {
    n_rows: usize,
    n_cols: usize,
    n_entries: usize,
    buffer: AlignedVec<T>, // contiguous backing storage: n_rows * n_cols * n_entries
}

impl<T: GenType> Grid<T> {
    // Create a grid of buffers with dimensions [n_rows, n_cols], which
    // has cells pointing to an array of [n_entries].
    pub fn new(n_rows: usize, n_cols: usize, n_entries: usize) -> Self {
        let total = n_rows * n_cols * n_entries;
        let buffer = AlignedVec::new(total, ALIGNMENT);
        Grid {
            n_rows,
            n_cols,
            n_entries,
            buffer,
        }
    }

    pub fn get(&self, row: usize, col: usize) -> &[T] {
        let idx = (row * self.n_cols + col) * self.n_entries;
        &self.buffer.get()[idx..idx + self.n_entries]
    }

    pub fn get_mut(&mut self, row: usize, col: usize) -> &mut [T] {
        let idx = (row * self.n_cols + col) * self.n_entries;
        &mut self.buffer.get_mut()[idx..idx + self.n_entries]
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.buffer.get_mut()
    }
}
#[repr(C, align(64))]
pub struct Cube<T: GenType> {
    dim1: usize,
    dim2: usize,
    dim3: usize,
    n_entries: usize,
    buffer: AlignedVec<T>, // contiguous: dim1 * dim2 * dim3 * n_entries
}

impl<T: GenType> Cube<T> {
    // Create a cube of buffers with dimensions [dim1, dim2, dim3], which
    // has cells pointing to an array of [n_entries].
    pub fn new(dim1: usize, dim2: usize, dim3: usize, n_entries: usize) -> Self {
        let total = dim1 * dim2 * dim3 * n_entries;
        let buffer = AlignedVec::new(total, ALIGNMENT);
        Cube {
            dim1,
            dim2,
            dim3,
            n_entries,
            buffer,
        }
    }
    pub fn get(&self, d1: usize, d2: usize, d3: usize) -> &[T] {
        let idx = (d3 * self.dim1 * self.dim2 + d1 * self.dim2 + d2) * self.n_entries;
        &self.buffer.get()[idx..idx + self.n_entries]
    }

    pub fn get_mut(&mut self, d1: usize, d2: usize, d3: usize) -> &mut [T] {
        let idx = (d3 * self.dim1 * self.dim2 + d1 * self.dim2 + d2) * self.n_entries;
        &mut self.buffer.get_mut()[idx..idx + self.n_entries]
    }

    pub fn flat_slice(&self) -> &[T] {
        self.buffer.get()
    }
}

#[repr(C, align(64))]
pub struct MultiVector<T: GenType> {
    buffer: Vec<Vec<T>>,
}

impl<T: GenType> MultiVector<T> {
    pub fn new(rows: usize) -> Self {
        let mut new_buf = Vec::new();
        for _ in 0..rows {
            let buf: Vec<T> = Vec::new();
            new_buf.push(buf);
        }

        MultiVector { buffer: new_buf }
    }

    pub fn rows(&self) -> usize {
        self.buffer.len()
    }

    pub fn length(&self) -> usize {
        self.buffer[0].len()
    }
}
// Implement dimension indexing for MultiVector
impl<T: GenType> Index<usize> for MultiVector<T> {
    type Output = Vec<T>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buffer[index]
    }
}
impl<T: GenType> IndexMut<usize> for MultiVector<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.buffer[index]
    }
}

#[repr(C, align(64))]
pub struct AlignedVec<T: GenType> {
    buffer: Vec<T>,
}

impl<T: GenType> AlignedVec<T> {
    pub fn new(size: usize, alignment: usize) -> Self {
        let buffer = Self::create_aligned_vec(size, alignment);
        AlignedVec { buffer }
    }

    pub fn get(&self) -> &[T] {
        self.as_slice()
    }

    pub fn get_mut(&mut self) -> &mut [T] {
        self.as_mut_slice()
    }

    fn create_aligned_vec(size: usize, alignment: usize) -> Vec<T> {
        let mut ptr: *mut libc::c_void = std::ptr::null_mut();
        let align_res =
            unsafe { libc::posix_memalign(&mut ptr, alignment, size * mem::size_of::<T>()) };
        if align_res != 0 {
            panic!("Failed to allocate aligned memory");
        }
        let ptr = ptr as *mut T;
        unsafe {
            for i in 0..size {
                ptr.add(i).write(T::default());
            }
            Vec::from_raw_parts(ptr, size, size)
        }
    }
}

// Provide slice accessors instead of exposing Vec directly
impl<T: GenType> AlignedVec<T> {
    pub fn as_slice(&self) -> &[T] {
        &self.buffer
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.buffer
    }
}

pub struct MemMappedVec<T: GenType> {
    buffer: Vec<T>,
}

impl<T: GenType> MemMappedVec<T> {
    pub fn new(size: usize, address: Option<u64>) -> Self {
        let buffer = Self::create_mmapped_vec(size, address);
        MemMappedVec { buffer }
    }

    pub fn get(&self) -> &Vec<T> {
        &self.buffer
    }

    pub fn get_mut(&mut self) -> &mut Vec<T> {
        &mut self.buffer
    }

    fn create_mmapped_vec(size: usize, address: Option<u64>) -> Vec<T> {
        let total_size = size * mem::size_of::<T>();

        // Use mmap to allocate memory at a specified address
        let ptr = unsafe {
            if let Some(addr) = address {
                let addr_ptr = addr as *mut libc::c_void;
                mmap(
                    addr_ptr,
                    total_size,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
                    -1,
                    0,
                )
            } else {
                mmap(
                    std::ptr::null_mut(),
                    total_size,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS,
                    -1,
                    0,
                )
            }
        };

        if ptr == MAP_FAILED {
            println!("Error produced by mmap: {}", ptr as isize);
            panic!("Failed to allocate memory using mmap");
        }

        // Check if the allocated memory matches the specified address
        if let Some(addr) = address {
            let addr_ptr = addr as *mut libc::c_void;
            if ptr != addr_ptr as *mut libc::c_void {
                println!("Memory was not allocated at the specified address");
                println!(
                    "Requested address: {:p} , Allocated address: {:p}",
                    addr_ptr, ptr
                );
            } else {
                println!("Memory allocated at the specified address: {:p}", ptr);
            }
        }

        let ptr = ptr as *mut T;
        unsafe {
            // Initialize the memory with default values
            for i in 0..size {
                ptr.add(i).write(T::default());
            }
            Vec::from_raw_parts(ptr, size, size)
        }
    }
}

impl<T: GenType> Drop for MemMappedVec<T> {
    fn drop(&mut self) {
        let buffer = std::mem::ManuallyDrop::new(std::mem::replace(&mut self.buffer, Vec::new()));
        let ptr = buffer.as_ptr() as *mut libc::c_void;
        let total_size = buffer.len() * std::mem::size_of::<T>();

        unsafe {
            libc::munmap(ptr, total_size);
        }
    }
}
