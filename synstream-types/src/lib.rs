use lazy_static::lazy_static;
use parking_lot::RwLock;
use rapidhash::{HashMapExt, RapidHashMap};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

// Re-export num_complex Complex types for convenience
pub use num_complex::{Complex32, Complex64};

#[derive(Clone)]
pub enum CmTypes {
    // Small Copy types - remain unboxed for efficiency (≤8 bytes)
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Char(char),
    Usize(usize),
    Isize(isize),

    // Large or heap-allocated types - wrapped in Arc for cheap cloning
    I128(Arc<i128>),
    U128(Arc<u128>),
    String(Arc<str>),
    Complex32(Arc<C32>),
    Complex64(Arc<C64>),
    VecCmt(Arc<Vec<CmTypes>>),

    // Special types
    Ref(usize),
    Res(usize),
    Barrier(Arc<str>),
    None,
    Init,
    // Note: Any and AnySliced are not deserialized via the custom Deserialize impl
    Any(Arc<RwLock<Box<dyn Any + Send + Sync>>>),
    AnySliced(Arc<dyn SlicedAccess>),
    /// Vector of Any types (for multi-socket initialization)
    VecAny(Arc<RwLock<Vec<Box<dyn Any + Send + Sync>>>>),
    /// Raw byte buffer — cheap-clone packet data via Arc (no RwLock/Box overhead).
    /// Used by network receiver path instead of from_any(Vec<u8>).
    Bytes(Arc<Vec<u8>>),
}

// Custom Deserialize implementation for CmTypes to handle Arc-wrapped types
impl<'de> Deserialize<'de> for CmTypes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Define a temporary enum with the same structure but without Arc
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum CmTypesHelper {
            Bool(bool),
            I8(i8),
            I16(i16),
            I32(i32),
            I64(i64),
            I128(i128),
            U8(u8),
            U16(u16),
            U32(u32),
            U64(u64),
            U128(u128),
            F32(f32),
            F64(f64),
            Char(char),
            Usize(usize),
            Isize(isize),
            String(std::string::String),
            Ref(usize),
            Res(usize),
            Barrier(std::string::String),
            Complex32(C32),
            Complex64(C64),
            VecCmt(Vec<CmTypes>),
        }

        let helper = CmTypesHelper::deserialize(deserializer)?;

        Ok(match helper {
            CmTypesHelper::Bool(v) => CmTypes::Bool(v),
            CmTypesHelper::I8(v) => CmTypes::I8(v),
            CmTypesHelper::I16(v) => CmTypes::I16(v),
            CmTypesHelper::I32(v) => CmTypes::I32(v),
            CmTypesHelper::I64(v) => CmTypes::I64(v),
            CmTypesHelper::I128(v) => CmTypes::I128(Arc::new(v)),
            CmTypesHelper::U8(v) => CmTypes::U8(v),
            CmTypesHelper::U16(v) => CmTypes::U16(v),
            CmTypesHelper::U32(v) => CmTypes::U32(v),
            CmTypesHelper::U64(v) => CmTypes::U64(v),
            CmTypesHelper::U128(v) => CmTypes::U128(Arc::new(v)),
            CmTypesHelper::F32(v) => CmTypes::F32(v),
            CmTypesHelper::F64(v) => CmTypes::F64(v),
            CmTypesHelper::Char(v) => CmTypes::Char(v),
            CmTypesHelper::Usize(v) => CmTypes::Usize(v),
            CmTypesHelper::Isize(v) => CmTypes::Isize(v),
            CmTypesHelper::String(v) => CmTypes::String(Arc::from(v.as_str())),
            CmTypesHelper::Ref(v) => CmTypes::Ref(v),
            CmTypesHelper::Res(v) => CmTypes::Res(v),
            CmTypesHelper::Barrier(v) => CmTypes::Barrier(Arc::from(v.as_str())),
            CmTypesHelper::Complex32(v) => CmTypes::Complex32(Arc::new(v)),
            CmTypesHelper::Complex64(v) => CmTypes::Complex64(Arc::new(v)),
            CmTypesHelper::VecCmt(v) => CmTypes::VecCmt(Arc::new(v)),
        })
    }
}

// Wrapper for Complex32 to implement Serialize/Deserialize
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct C32(pub Complex32);

impl Serialize for C32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Complex32", 2)?;
        state.serialize_field("re", &self.0.re)?;
        state.serialize_field("im", &self.0.im)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for C32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Complex32Fields {
            re: f32,
            im: f32,
        }

        let fields = Complex32Fields::deserialize(deserializer)?;
        Ok(C32(Complex32::new(fields.re, fields.im)))
    }
}

impl From<Complex32> for C32 {
    fn from(c: Complex32) -> Self {
        C32(c)
    }
}

impl From<C32> for Complex32 {
    fn from(c: C32) -> Self {
        c.0
    }
}

// Wrapper for Complex64 to implement Serialize/Deserialize
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct C64(pub Complex64);

impl Serialize for C64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Complex64", 2)?;
        state.serialize_field("re", &self.0.re)?;
        state.serialize_field("im", &self.0.im)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for C64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Complex64Fields {
            re: f64,
            im: f64,
        }

        let fields = Complex64Fields::deserialize(deserializer)?;
        Ok(C64(Complex64::new(fields.re, fields.im)))
    }
}

impl From<Complex64> for C64 {
    fn from(c: Complex64) -> Self {
        C64(c)
    }
}

impl From<C64> for Complex64 {
    fn from(c: C64) -> Self {
        c.0
    }
}

impl CmTypes {
    // Default CmPtr pointer
    pub fn default_pointer() -> CmPtr {
        |_: &[CmTypes]| CmTypes::None
    }

    // Helper constructors for Arc-wrapped types - more ergonomic than calling Arc::new directly
    pub fn new_string<S: AsRef<str>>(s: S) -> Self {
        CmTypes::String(Arc::from(s.as_ref()))
    }

    pub fn new_i128(val: i128) -> Self {
        CmTypes::I128(Arc::new(val))
    }

    pub fn new_u128(val: u128) -> Self {
        CmTypes::U128(Arc::new(val))
    }

    pub fn new_complex32(val: C32) -> Self {
        CmTypes::Complex32(Arc::new(val))
    }

    pub fn new_complex64(val: C64) -> Self {
        CmTypes::Complex64(Arc::new(val))
    }

    pub fn new_vec(vec: Vec<CmTypes>) -> Self {
        CmTypes::VecCmt(Arc::new(vec))
    }

    pub fn new_barrier<S: AsRef<str>>(s: S) -> Self {
        CmTypes::Barrier(Arc::from(s.as_ref()))
    }

    // Helper methods to extract values from Arc-wrapped types
    /// Extract String from Arc<str> - for compatibility with existing code
    pub fn to_string_owned(&self) -> String {
        match self {
            CmTypes::String(s) => s.to_string(),
            CmTypes::Barrier(s) => s.to_string(),
            _ => format!("{}", self),
        }
    }

    /// Extract String from Arc<str>
    pub fn as_string(&self) -> Option<String> {
        match self {
            CmTypes::String(s) => Some(s.to_string()),
            _ => None,
        }
    }

    /// Get reference to str from Arc<str>
    pub fn as_str(&self) -> Option<&str> {
        match self {
            CmTypes::String(s) => Some(s),
            CmTypes::Barrier(s) => Some(s),
            _ => None,
        }
    }

    /// Extract i128 value
    pub fn as_i128(&self) -> Option<i128> {
        match self {
            CmTypes::I128(v) => Some(**v),
            _ => None,
        }
    }

    /// Extract u128 value
    pub fn as_u128(&self) -> Option<u128> {
        match self {
            CmTypes::U128(v) => Some(**v),
            _ => None,
        }
    }

    /// Get reference to Vec<CmTypes>
    pub fn as_vec(&self) -> Option<&Vec<CmTypes>> {
        match self {
            CmTypes::VecCmt(v) => Some(v),
            _ => None,
        }
    }

    /// Extract Complex32 value
    pub fn as_complex32(&self) -> Option<C32> {
        match self {
            CmTypes::Complex32(c) => Some(**c),
            _ => None,
        }
    }

    /// Extract Complex64 value
    pub fn as_complex64(&self) -> Option<C64> {
        match self {
            CmTypes::Complex64(c) => Some(**c),
            _ => None,
        }
    }

    /// Wrap raw bytes in Arc — cheap cloning, no RwLock/Box overhead.
    /// Use this instead of `from_any(Vec<u8>)` in performance-critical paths.
    #[inline]
    pub fn from_bytes(bytes: Vec<u8>) -> CmTypes {
        CmTypes::Bytes(Arc::new(bytes))
    }

    /// Access byte data via closure — works with both `Bytes` and `Any(Vec<u8>)`.
    /// For `Bytes`: direct reference (zero overhead).
    /// For `Any(Vec<u8>)`: acquires RwLock read guard (backward compatible).
    #[inline]
    pub fn with_bytes<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&[u8]) -> R,
    {
        match self {
            CmTypes::Bytes(bytes) => Some(f(bytes.as_slice())),
            CmTypes::Any(lock) => {
                let guard = lock.read();
                guard.downcast_ref::<Vec<u8>>().map(|bytes| f(bytes.as_slice()))
            }
            _ => None,
        }
    }

    pub fn from_any<T: Any + Send + Sync>(value: T) -> CmTypes {
        CmTypes::Any(Arc::new(RwLock::new(Box::new(value))))
    }

    /// Create from vector of Any types (for multi-socket initialization)
    pub fn from_any_vec<T: Any + Send + Sync>(vec: Vec<T>) -> CmTypes {
        let boxed_vec: Vec<Box<dyn Any + Send + Sync>> = vec
            .into_iter()
            .map(|item| Box::new(item) as Box<dyn Any + Send + Sync>)
            .collect();
        CmTypes::VecAny(Arc::new(RwLock::new(boxed_vec)))
    }

    /// Extract vector and downcast to specific type
    /// Returns None if types don't match
    pub fn as_vec_any<T: Any + Clone>(&self) -> Option<Vec<T>> {
        if let CmTypes::VecAny(lock) = self {
            let guard = lock.read();
            let mut result = Vec::with_capacity(guard.len());
            for boxed in guard.iter() {
                match boxed.downcast_ref::<T>() {
                    Some(value) => result.push(value.clone()),
                    None => {
                        println!(
                            "VecAny downcast failed: Expected type '{}', but got type '{:?}'",
                            std::any::type_name::<T>(),
                            std::any::type_name_of_val(boxed.as_ref())
                        );
                        return None;
                    }
                }
            }
            Some(result)
        } else {
            None
        }
    }

    /// Get length of VecAny without extracting elements
    pub fn vec_any_len(&self) -> Option<usize> {
        if let CmTypes::VecAny(lock) = self {
            Some(lock.read().len())
        } else {
            None
        }
    }

    /// Read-only borrow
    pub fn with_any<T: Any + Send + Sync, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&T) -> R,
    {
        if let CmTypes::Any(lock) = self {
            let guard = lock.read();
            match guard.downcast_ref::<T>() {
                Some(value) => Some(f(value)),
                None => {
                    // Print both expected and actual type for debugging
                    println!(
                        "Downcast failed: Expected type '{}', but got type '{:?}'",
                        std::any::type_name::<T>(),
                        std::any::type_name_of_val(guard.as_ref())
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    /// Mutable borrow
    pub fn with_any_mut<T: Any + Send + Sync, F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut T) -> R,
    {
        if let CmTypes::Any(lock) = self {
            let mut guard = lock.write();
            match guard.downcast_mut::<T>() {
                Some(value) => Some(f(value)),
                None => {
                    // Print both expected and actual type for debugging
                    println!(
                        "Downcast failed: Expected type '{}', but got type '{:?}'",
                        std::any::type_name::<T>(),
                        std::any::type_name_of_val(guard.as_ref())
                    );
                    None
                }
            }
        } else {
            None
        }
    }

    pub fn from_any_sliced<T: Any + Send + Sync + Sliceable<U>, U: Clone + 'static>(
        mut value: T,
    ) -> CmTypes {
        // Get the mutable slice from the value
        let slice = value.as_mut_slice();
        let total_length = slice.len();
        let data_ptr = SendPtr(slice.as_mut_ptr());

        // Box the original value to keep it alive
        let boxed_value: Box<dyn Any + Send + Sync> = Box::new(value);

        // Create a sliced container that holds the data with write bits tracking
        let container = SlicedContainer::new(boxed_value, data_ptr, total_length);

        CmTypes::AnySliced(Arc::new(container))
    }

    /// Get the total length in an AnySliced CmTypes
    pub fn sliced_total_length(&self) -> Option<usize> {
        if let CmTypes::AnySliced(container_arc) = self {
            Some(container_arc.total_length())
        } else {
            None
        }
    }

    /// Check if a range is borrowed in an AnySliced CmTypes
    pub fn is_sliced_range_borrowed(&self, start: usize, len: usize) -> Option<bool> {
        if let CmTypes::AnySliced(container_arc) = self {
            Some(container_arc.is_range_borrowed(start, len))
        } else {
            None
        }
    }

    /// Unset a range in an AnySliced CmTypes
    pub fn unset_sliced_range(&self, start: usize, len: usize) {
        if let CmTypes::AnySliced(container_arc) = self {
            container_arc.unset_range(start, len);
        }
    }

    /// Get mutable access to a range in an AnySliced CmTypes with automatic cleanup
    /// Returns a RAII guard that automatically releases write bits when dropped
    pub fn sliced_get_mut_range<T>(
        &self,
        start: usize,
        len: usize,
    ) -> Option<MutSliceGuard<'_, T>> {
        if let CmTypes::AnySliced(container_arc) = self {
            unsafe {
                if let Some((raw_ptr, byte_len)) = container_arc.get_mut_range_raw(start, len) {
                    let element_size = std::mem::size_of::<T>();
                    if element_size == 0 || byte_len % element_size != 0 {
                        // Release the bits since we can't use them
                        container_arc.unset_range(start, len);
                        return None;
                    }
                    let element_len = byte_len / element_size;
                    let typed_ptr = raw_ptr as *mut T;
                    let slice = std::slice::from_raw_parts_mut(typed_ptr, element_len);

                    Some(MutSliceGuard {
                        slice,
                        container: container_arc.as_ref(),
                        start,
                        len,
                    })
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get mutable access with a timeout and retry backoff between attempts.
    pub fn sliced_get_mut_range_timeout<T>(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
        retry: Duration,
    ) -> Option<MutSliceGuard<'_, T>> {
        if let CmTypes::AnySliced(container_arc) = self {
            unsafe {
                if let Some((raw_ptr, byte_len)) =
                    container_arc.get_mut_range_raw_timeout(start, len, timeout, retry)
                {
                    let element_size = std::mem::size_of::<T>();
                    if element_size == 0 || byte_len % element_size != 0 {
                        container_arc.unset_range(start, len);
                        return None;
                    }
                    let element_len = byte_len / element_size;
                    let typed_ptr = raw_ptr as *mut T;
                    let slice = std::slice::from_raw_parts_mut(typed_ptr, element_len);

                    Some(MutSliceGuard {
                        slice,
                        container: container_arc.as_ref(),
                        start,
                        len,
                    })
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    /// Returns a raw mutable pointer to the inner type `T` if it matches.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The returned pointer is not used to create multiple mutable references
    /// - The pointer is not dereferenced after the `CmTypes` value is dropped
    /// - Proper synchronization is used when accessing the data from multiple threads
    pub unsafe fn as_mut_ptr<T: Any + Send + Sync>(&self) -> Option<SendPtr<T>> {
        if let CmTypes::Any(lock) = self {
            let guard = lock.read();
            guard
                .downcast_ref::<T>()
                .map(|r| SendPtr(r as *const T as *mut T))
        } else {
            None
        }
    }

    pub fn valid_number_to_usize(&self) -> Option<usize> {
        match self {
            CmTypes::Usize(x) => Some(*x),
            CmTypes::U8(x) => Some(*x as usize),
            CmTypes::U16(x) => Some(*x as usize),
            CmTypes::U32(x) => Some(*x as usize),
            CmTypes::U64(x) => Some(*x as usize),
            CmTypes::U128(x) => Some(**x as usize),
            CmTypes::I8(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I16(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I32(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I64(x) if *x >= 0 => Some(*x as usize),
            CmTypes::I128(x) if **x >= 0 => Some(**x as usize),
            _ => None,
        }
    }

    pub fn is_result(&self) -> bool {
        matches!(self, CmTypes::Res(_))
    }

    pub fn is_reference(&self) -> bool {
        matches!(self, CmTypes::Ref(_))
    }

    pub fn is_barrier(&self) -> bool {
        matches!(self, CmTypes::Barrier(_))
    }

    /// Get a borrowed reference to a range without setting write bits
    /// Returns Some(&[T]) if no mutable borrows are active in the range, None otherwise
    pub fn sliced_get_range<T>(&self, start: usize, len: usize) -> Option<&[T]> {
        if let CmTypes::AnySliced(container_arc) = self {
            unsafe {
                if let Some((raw_ptr, byte_len)) = container_arc.get_range_raw(start, len) {
                    let element_size = std::mem::size_of::<T>();
                    if element_size == 0 || byte_len % element_size != 0 {
                        return None;
                    }
                    let element_len = byte_len / element_size;
                    let typed_ptr = raw_ptr as *const T;
                    let slice = std::slice::from_raw_parts(typed_ptr, element_len);
                    Some(slice)
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    /// Get a borrowed reference to a range with timeout
    /// Waits up to the specified duration for mutable borrows to be released
    /// Returns Some(&[T]) if successful, None if timeout or out of bounds
    pub fn sliced_get_range_timeout<T>(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
    ) -> Option<&[T]> {
        if let CmTypes::AnySliced(container_arc) = self {
            unsafe {
                if let Some((raw_ptr, byte_len)) =
                    container_arc.get_range_raw_timeout(start, len, timeout)
                {
                    let element_size = std::mem::size_of::<T>();
                    if element_size == 0 || byte_len % element_size != 0 {
                        return None;
                    }
                    let element_len = byte_len / element_size;
                    let typed_ptr = raw_ptr as *const T;
                    let slice = std::slice::from_raw_parts(typed_ptr, element_len);
                    Some(slice)
                } else {
                    None
                }
            }
        } else {
            None
        }
    }

    /// Non-blocking check if a range can be read without conflicts
    /// Returns true if sliced_get_range() would succeed for this range
    pub fn can_read_range(&self, start: usize, len: usize) -> bool {
        if let CmTypes::AnySliced(container_arc) = self {
            container_arc.can_read_range(start, len)
        } else {
            false
        }
    }
}

/// A wrapper that makes raw pointers `Send`/`Sync`.
pub struct SendPtr<T>(pub *mut T);

/// RAII guard for automatic release of write bits
pub struct MutSliceGuard<'a, T> {
    slice: &'a mut [T],
    container: &'a dyn SlicedAccess,
    start: usize,
    len: usize,
}

impl<'a, T> std::ops::Deref for MutSliceGuard<'a, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

impl<'a, T> std::ops::DerefMut for MutSliceGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slice
    }
}

impl<'a, T> Drop for MutSliceGuard<'a, T> {
    fn drop(&mut self) {
        // Automatically release write bits when guard is dropped
        self.container.unset_range(self.start, self.len);
    }
}

/// Trait for type-erased sliced access
pub trait SlicedAccess: Any + Send + Sync {
    fn total_length(&self) -> usize;
    fn is_range_borrowed(&self, start: usize, len: usize) -> bool;
    fn unset_range(&self, start: usize, len: usize);
    /// Get raw mutable pointer and length for a range, returns None if range is already borrowed
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The returned pointer is only used within the valid range `[start, start + len)`
    /// - No other mutable references exist to the same range
    /// - The pointer is not used after calling `unset_range` for this range
    unsafe fn get_mut_range_raw(&self, start: usize, len: usize) -> Option<(*mut u8, usize)>;

    /// Get raw mutable pointer with timeout and optional backoff between retries
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The returned pointer is only used within the valid range `[start, start + len)`
    /// - No other mutable references exist to the same range
    /// - The pointer is not used after calling `unset_range` for this range
    unsafe fn get_mut_range_raw_timeout(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
        retry: Duration,
    ) -> Option<(*mut u8, usize)>;

    /// Get raw immutable pointer and length for a range without setting write bits
    /// Returns None if any mutable borrows are active in the range
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The returned pointer is only used within the valid range `[start, start + len)`
    /// - No mutable references are created to the same range while this pointer is in use
    unsafe fn get_range_raw(&self, start: usize, len: usize) -> Option<(*const u8, usize)>;

    /// Get raw immutable pointer with timeout
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The returned pointer is only used within the valid range `[start, start + len)`
    /// - No mutable references are created to the same range while this pointer is in use
    unsafe fn get_range_raw_timeout(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
    ) -> Option<(*const u8, usize)>;

    /// Non-blocking check if a range can be read without conflicts
    fn can_read_range(&self, start: usize, len: usize) -> bool;
}

unsafe impl<T> Send for SendPtr<T> {}
unsafe impl<T> Sync for SendPtr<T> {}

pub trait Sliceable<T> {
    fn as_mut_slice(&mut self) -> &mut [T];
}

// A sliced container that tracks mutable borrows with chunked write bitsets
pub struct SlicedContainer<T> {
    data_ptr: SendPtr<T>,
    total_length: usize,
    write_chunks: Vec<AtomicU64>,
    _data: Box<dyn Any + Send + Sync>,
}

impl<T> SlicedContainer<T> {
    const CHUNK_BITS: usize = u64::BITS as usize;

    /// Create a new sliced container with write bits tracking
    pub fn new(
        value: Box<dyn Any + Send + Sync>,
        data_ptr: SendPtr<T>,
        total_length: usize,
    ) -> Self {
        let chunk_count = total_length.div_ceil(Self::CHUNK_BITS);
        let write_chunks = (0..chunk_count).map(|_| AtomicU64::new(0)).collect();

        Self {
            data_ptr,
            total_length,
            write_chunks,
            _data: value,
        }
    }

    #[inline]
    fn range_end(&self, start: usize, len: usize) -> Option<usize> {
        if start > self.total_length {
            return None;
        }
        len.checked_add(start)
            .filter(|end| *end <= self.total_length)
    }

    #[inline]
    fn mask_for_chunk(range_start: usize, range_end: usize, chunk_idx: usize) -> u64 {
        let chunk_start = chunk_idx * Self::CHUNK_BITS;
        let chunk_end = chunk_start + Self::CHUNK_BITS;

        let from = range_start.max(chunk_start);
        let to = range_end.min(chunk_end);
        if from >= to {
            return 0;
        }

        let bit_start = from - chunk_start;
        let bit_len = to - from;
        if bit_len >= Self::CHUNK_BITS {
            return u64::MAX;
        }

        let base = u64::MAX >> (Self::CHUNK_BITS - bit_len);
        base << bit_start
    }

    /// Get mutable access to a range of the buffer using chunked atomic bitsets
    /// Returns a mutable slice reference if successful, None if any part of the range overlaps
    #[allow(clippy::mut_from_ref)] // Intentional: uses atomic bitsets for interior mutability tracking
    pub fn sliced_data_mut(&self, start: usize, len: usize) -> Option<&mut [T]> {
        let range_end = self.range_end(start, len)?;

        if len == 0 {
            unsafe {
                let ptr = self.data_ptr.0.add(start);
                return Some(std::slice::from_raw_parts_mut(ptr, 0));
            }
        }

        let first_chunk = start / Self::CHUNK_BITS;
        let last_chunk = (range_end - 1) / Self::CHUNK_BITS;
        let mut acquired: Vec<(usize, u64)> = Vec::with_capacity(last_chunk - first_chunk + 1);

        for chunk_idx in first_chunk..=last_chunk {
            let mask = Self::mask_for_chunk(start, range_end, chunk_idx);
            if mask == 0 {
                continue;
            }

            let chunk = &self.write_chunks[chunk_idx];
            let mut current = chunk.load(Ordering::Acquire);
            loop {
                if current & mask != 0 {
                    for (idx, release_mask) in acquired.into_iter() {
                        self.write_chunks[idx].fetch_and(!release_mask, Ordering::Release);
                    }
                    return None;
                }

                let next = current | mask;
                match chunk.compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        acquired.push((chunk_idx, mask));
                        break;
                    }
                    Err(actual) => current = actual,
                }
            }
        }

        unsafe {
            let ptr = self.data_ptr.0.add(start);
            Some(std::slice::from_raw_parts_mut(ptr, len))
        }
    }

    /// Try to acquire mutable access with retries until timeout expires.
    pub fn sliced_data_mut_timeout(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
        retry: Duration,
    ) -> Option<&mut [T]> {
        let _ = self.range_end(start, len)?;

        if timeout.as_nanos() == 0 {
            return self.sliced_data_mut(start, len);
        }

        let sleep_dur = if retry.as_nanos() == 0 {
            Duration::from_micros(100)
        } else {
            retry
        };

        let start_time = std::time::Instant::now();
        loop {
            if let Some(slice) = self.sliced_data_mut(start, len) {
                return Some(slice);
            }

            if start_time.elapsed() >= timeout {
                return None;
            }

            thread::sleep(sleep_dur);
        }
    }

    /// Manually unset write bits for a range
    pub fn unset_range(&self, start: usize, len: usize) {
        let Some(range_end) = self.range_end(start, len) else {
            return;
        };

        if len == 0 {
            return;
        }

        let first_chunk = start / Self::CHUNK_BITS;
        let last_chunk = (range_end - 1) / Self::CHUNK_BITS;
        for chunk_idx in first_chunk..=last_chunk {
            let mask = Self::mask_for_chunk(start, range_end, chunk_idx);
            if mask != 0 {
                self.write_chunks[chunk_idx].fetch_and(!mask, Ordering::Release);
            }
        }
    }

    /// Get the total length of the buffer
    pub fn total_length(&self) -> usize {
        self.total_length
    }

    /// Check if a range is currently borrowed
    pub fn is_range_borrowed(&self, start: usize, len: usize) -> bool {
        let Some(range_end) = self.range_end(start, len) else {
            return false;
        };

        if len == 0 {
            return false;
        }

        let first_chunk = start / Self::CHUNK_BITS;
        let last_chunk = (range_end - 1) / Self::CHUNK_BITS;
        for chunk_idx in first_chunk..=last_chunk {
            let mask = Self::mask_for_chunk(start, range_end, chunk_idx);
            if mask != 0 && (self.write_chunks[chunk_idx].load(Ordering::Acquire) & mask) != 0 {
                return true;
            }
        }
        false
    }

    /// Get a borrowed reference to a range without setting write bits
    /// Returns Some(&[T]) if no mutable borrows are active in the range, None otherwise
    /// This allows multiple concurrent read-only accesses to the same data
    pub fn sliced_get_range(&self, start: usize, len: usize) -> Option<&[T]> {
        let range_end = self.range_end(start, len)?;

        if len == 0 {
            unsafe {
                let ptr = self.data_ptr.0.add(start);
                return Some(std::slice::from_raw_parts(ptr, 0));
            }
        }

        let first_chunk = start / Self::CHUNK_BITS;
        let last_chunk = (range_end - 1) / Self::CHUNK_BITS;
        for chunk_idx in first_chunk..=last_chunk {
            let mask = Self::mask_for_chunk(start, range_end, chunk_idx);
            if mask != 0 && (self.write_chunks[chunk_idx].load(Ordering::Acquire) & mask) != 0 {
                return None;
            }
        }

        unsafe {
            let ptr = self.data_ptr.0.add(start);
            Some(std::slice::from_raw_parts(ptr, len))
        }
    }

    /// Get a borrowed reference to a range with timeout
    /// Waits up to the specified duration for mutable borrows to be released
    /// Returns Some(&[T]) if successful, None if timeout or out of bounds
    pub fn sliced_get_range_timeout(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
    ) -> Option<&[T]> {
        let range_end = self.range_end(start, len)?;

        if len == 0 {
            unsafe {
                let ptr = self.data_ptr.0.add(start);
                return Some(std::slice::from_raw_parts(ptr, 0));
            }
        }

        let start_time = std::time::Instant::now();

        loop {
            let first_chunk = start / Self::CHUNK_BITS;
            let last_chunk = (range_end - 1) / Self::CHUNK_BITS;
            let mut clear = true;
            for chunk_idx in first_chunk..=last_chunk {
                let mask = Self::mask_for_chunk(start, range_end, chunk_idx);
                if mask != 0 && (self.write_chunks[chunk_idx].load(Ordering::Acquire) & mask) != 0 {
                    clear = false;
                    break;
                }
            }

            if clear {
                unsafe {
                    let ptr = self.data_ptr.0.add(start);
                    return Some(std::slice::from_raw_parts(ptr, len));
                }
            }

            // Check if we've exceeded the timeout
            if start_time.elapsed() >= timeout {
                return None;
            }

            // Small delay before retrying
            thread::sleep(Duration::from_nanos(100));
        }
    }

    /// Non-blocking check if a range can be read without conflicts
    /// Returns true if sliced_get_range() would succeed for this range
    pub fn can_read_range(&self, start: usize, len: usize) -> bool {
        let Some(range_end) = self.range_end(start, len) else {
            return false;
        };

        if len == 0 {
            return true;
        }

        let first_chunk = start / Self::CHUNK_BITS;
        let last_chunk = (range_end - 1) / Self::CHUNK_BITS;
        for chunk_idx in first_chunk..=last_chunk {
            let mask = Self::mask_for_chunk(start, range_end, chunk_idx);
            if mask != 0 && (self.write_chunks[chunk_idx].load(Ordering::Acquire) & mask) != 0 {
                return false;
            }
        }
        true
    }
}

impl<T: 'static> SlicedAccess for SlicedContainer<T> {
    fn total_length(&self) -> usize {
        self.total_length()
    }

    fn is_range_borrowed(&self, start: usize, len: usize) -> bool {
        self.is_range_borrowed(start, len)
    }

    fn unset_range(&self, start: usize, len: usize) {
        self.unset_range(start, len)
    }

    unsafe fn get_mut_range_raw(&self, start: usize, len: usize) -> Option<(*mut u8, usize)> {
        // First try to get the mutable slice using our existing method
        if let Some(slice) = self.sliced_data_mut(start, len) {
            let ptr = slice.as_mut_ptr() as *mut u8;
            let byte_len = len * std::mem::size_of::<T>();
            Some((ptr, byte_len))
        } else {
            None
        }
    }

    unsafe fn get_mut_range_raw_timeout(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
        retry: Duration,
    ) -> Option<(*mut u8, usize)> {
        if let Some(slice) = self.sliced_data_mut_timeout(start, len, timeout, retry) {
            let ptr = slice.as_mut_ptr() as *mut u8;
            let byte_len = len * std::mem::size_of::<T>();
            Some((ptr, byte_len))
        } else {
            None
        }
    }

    unsafe fn get_range_raw(&self, start: usize, len: usize) -> Option<(*const u8, usize)> {
        if let Some(slice) = self.sliced_get_range(start, len) {
            let ptr = slice.as_ptr() as *const u8;
            let byte_len = len * std::mem::size_of::<T>();
            Some((ptr, byte_len))
        } else {
            None
        }
    }

    unsafe fn get_range_raw_timeout(
        &self,
        start: usize,
        len: usize,
        timeout: Duration,
    ) -> Option<(*const u8, usize)> {
        if let Some(slice) = self.sliced_get_range_timeout(start, len, timeout) {
            let ptr = slice.as_ptr() as *const u8;
            let byte_len = len * std::mem::size_of::<T>();
            Some((ptr, byte_len))
        } else {
            None
        }
    }

    fn can_read_range(&self, start: usize, len: usize) -> bool {
        self.can_read_range(start, len)
    }
}

impl PartialEq for CmTypes {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CmTypes::Bool(a), CmTypes::Bool(b)) => a == b,
            (CmTypes::I8(a), CmTypes::I8(b)) => a == b,
            (CmTypes::I16(a), CmTypes::I16(b)) => a == b,
            (CmTypes::I32(a), CmTypes::I32(b)) => a == b,
            (CmTypes::I64(a), CmTypes::I64(b)) => a == b,
            (CmTypes::I128(a), CmTypes::I128(b)) => **a == **b,
            (CmTypes::U8(a), CmTypes::U8(b)) => a == b,
            (CmTypes::U16(a), CmTypes::U16(b)) => a == b,
            (CmTypes::U32(a), CmTypes::U32(b)) => a == b,
            (CmTypes::U64(a), CmTypes::U64(b)) => a == b,
            (CmTypes::U128(a), CmTypes::U128(b)) => **a == **b,
            (CmTypes::F32(a), CmTypes::F32(b)) => a == b,
            (CmTypes::F64(a), CmTypes::F64(b)) => a == b,
            (CmTypes::Char(a), CmTypes::Char(b)) => a == b,
            (CmTypes::Usize(a), CmTypes::Usize(b)) => a == b,
            (CmTypes::String(a), CmTypes::String(b)) => **a == **b,
            (CmTypes::VecCmt(a), CmTypes::VecCmt(b)) => **a == **b,
            (CmTypes::Ref(a), CmTypes::Ref(b)) => a == b,
            (CmTypes::Res(a), CmTypes::Res(b)) => a == b,
            (CmTypes::Barrier(a), CmTypes::Barrier(b)) => **a == **b,
            (CmTypes::Complex32(a), CmTypes::Complex32(b)) => **a == **b,
            (CmTypes::Complex64(a), CmTypes::Complex64(b)) => **a == **b,
            (CmTypes::None, CmTypes::None) => true,
            (CmTypes::Init, CmTypes::Init) => true,
            (CmTypes::Bytes(a), CmTypes::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl std::fmt::Debug for CmTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CmTypes::Bool(val) => write!(f, "Bool({:?})", val),
            CmTypes::I8(val) => write!(f, "I8({:?})", val),
            CmTypes::I16(val) => write!(f, "I16({:?})", val),
            CmTypes::I32(val) => write!(f, "I32({:?})", val),
            CmTypes::I64(val) => write!(f, "I64({:?})", val),
            CmTypes::I128(val) => write!(f, "I128({:?})", **val),
            CmTypes::U8(val) => write!(f, "U8({:?})", val),
            CmTypes::U16(val) => write!(f, "U16({:?})", val),
            CmTypes::U32(val) => write!(f, "U32({:?})", val),
            CmTypes::U64(val) => write!(f, "U64({:?})", val),
            CmTypes::U128(val) => write!(f, "U128({:?})", **val),
            CmTypes::F32(val) => write!(f, "F32({:?})", val),
            CmTypes::F64(val) => write!(f, "F64({:?})", val),
            CmTypes::Char(val) => write!(f, "Char({:?})", val),
            CmTypes::Usize(val) => write!(f, "Usize({:?})", val),
            CmTypes::Isize(val) => write!(f, "Isize({:?})", val),
            CmTypes::VecCmt(val) => write!(f, "VecCmt({:?})", **val),
            CmTypes::String(val) => write!(f, "String({:?})", &**val),
            CmTypes::Ref(val) => write!(f, "Ref({:?})", val),
            CmTypes::Res(val) => write!(f, "Res({:?})", val),
            CmTypes::Barrier(val) => write!(f, "Barrier({:?})", &**val),
            CmTypes::Complex32(val) => write!(f, "Complex32({:?})", **val),
            CmTypes::Complex64(val) => write!(f, "Complex64({:?})", **val),
            CmTypes::None => write!(f, "None"),
            CmTypes::Init => write!(f, "Init"),
            CmTypes::Any(_) => write!(f, "CustomType"),
            CmTypes::AnySliced(_) => write!(f, "SlicedType"),
            CmTypes::VecAny(lock) => {
                let guard = lock.read();
                write!(f, "VecAny[{}]", guard.len())
            }
            CmTypes::Bytes(val) => write!(f, "Bytes(len={})", val.len()),
        }
    }
}

// implement Display for CmTypes
impl fmt::Display for CmTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CmTypes::Bool(x) => write!(f, "{}", x),
            CmTypes::I8(x) => write!(f, "{}", x),
            CmTypes::I16(x) => write!(f, "{}", x),
            CmTypes::I32(x) => write!(f, "{}", x),
            CmTypes::I64(x) => write!(f, "{}", x),
            CmTypes::I128(x) => write!(f, "{}", **x),
            CmTypes::U8(x) => write!(f, "{}", x),
            CmTypes::U16(x) => write!(f, "{}", x),
            CmTypes::U32(x) => write!(f, "{}", x),
            CmTypes::U64(x) => write!(f, "{}", x),
            CmTypes::U128(x) => write!(f, "{}", **x),
            CmTypes::F32(x) => write!(f, "{}", x),
            CmTypes::F64(x) => write!(f, "{}", x),
            CmTypes::Char(x) => write!(f, "{}", x),
            CmTypes::Usize(x) => write!(f, "{}", x),
            CmTypes::Isize(x) => write!(f, "{}", x),
            CmTypes::String(x) => write!(f, "{}", &**x),
            CmTypes::Ref(x) => write!(f, "{}", x),
            CmTypes::Res(x) => write!(f, "{}", x),
            CmTypes::Barrier(x) => write!(f, "{}", &**x),
            CmTypes::Complex32(x) => {
                if x.0.im >= 0.0 {
                    write!(f, "{}+{}i", x.0.re, x.0.im)
                } else {
                    write!(f, "{}{}i", x.0.re, x.0.im)
                }
            }
            CmTypes::Complex64(x) => {
                if x.0.im >= 0.0 {
                    write!(f, "{}+{}i", x.0.re, x.0.im)
                } else {
                    write!(f, "{}{}i", x.0.re, x.0.im)
                }
            }
            CmTypes::None => write!(f, "None"),
            CmTypes::Init => write!(f, "Init"),
            CmTypes::VecCmt(x) => {
                write!(f, "[")?;
                for (i, item) in x.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            CmTypes::Any(_) => write!(f, "CustomType"),
            CmTypes::AnySliced(_) => write!(f, "CustomSlicedType"),
            CmTypes::VecAny(lock) => {
                let guard = lock.read();
                write!(f, "VecAny[{}]", guard.len())
            }
            CmTypes::Bytes(val) => write!(f, "Bytes[{}]", val.len()),
        }
    }
}

pub type CmPtr = fn(&[CmTypes]) -> CmTypes;

#[derive(Debug)]
pub struct CustomError {
    details: String,
}

impl CustomError {
    fn new(msg: &str) -> CustomError {
        CustomError {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for CustomError {
    fn description(&self) -> &str {
        &self.details
    }
}

/// Parse a Complex32 from various string formats:
/// - JSON: {"re":3.5,"im":2.5}
/// - Standard: 3.5+2.5i or 3.5-2.5i
/// - Comma-separated: 3.5,2.5
fn parse_complex32(s: &str) -> Result<C32, CustomError> {
    let s = s.trim();

    // Try JSON format first
    if s.starts_with('{') {
        return serde_json::from_str::<C32>(s)
            .map_err(|_| CustomError::new("invalid Complex32 JSON format"));
    }

    // Try comma-separated format: "3.5,2.5"
    if let Some(comma_pos) = s.find(',') {
        let re_str = s[..comma_pos].trim();
        let im_str = s[comma_pos + 1..].trim();

        let re = re_str
            .parse::<f32>()
            .map_err(|_| CustomError::new("invalid Complex32 real part"))?;
        let im = im_str
            .parse::<f32>()
            .map_err(|_| CustomError::new("invalid Complex32 imaginary part"))?;

        return Ok(C32(Complex32::new(re, im)));
    }

    // Try standard format: "3.5+2.5i" or "3.5-2.5i"
    // Find the position of 'i' at the end
    if let Some(s) = s.strip_suffix('i') {
        // Find the last '+' or '-' that's not at the start
        let mut split_pos = None;
        for (i, c) in s.char_indices().skip(1) {
            if c == '+' || c == '-' {
                split_pos = Some(i);
            }
        }

        if let Some(pos) = split_pos {
            let re_str = s[..pos].trim();
            let im_str = s[pos..].trim();

            let re = re_str
                .parse::<f32>()
                .map_err(|_| CustomError::new("invalid Complex32 real part"))?;
            let im = im_str
                .parse::<f32>()
                .map_err(|_| CustomError::new("invalid Complex32 imaginary part"))?;

            return Ok(C32(Complex32::new(re, im)));
        }
    }

    Err(CustomError::new(
        "invalid Complex32 format. Use: '3.5+2.5i', '3.5,2.5', or '{\"re\":3.5,\"im\":2.5}'",
    ))
}

/// Parse a Complex64 from various string formats:
/// - JSON: {"re":3.5,"im":2.5}
/// - Standard: 3.5+2.5i or 3.5-2.5i
fn parse_complex64(s: &str) -> Result<C64, CustomError> {
    let s = s.trim();

    // Try JSON format first
    if s.starts_with('{') {
        return serde_json::from_str::<C64>(s)
            .map_err(|_| CustomError::new("invalid Complex64 JSON format"));
    }

    // Try standard format: "3.5+2.5i" or "3.5-2.5i"
    // Find the position of 'i' at the end
    if let Some(s) = s.strip_suffix('i') {
        // Find the last '+' or '-' that's not at the start
        let mut split_pos = None;
        for (i, c) in s.char_indices().skip(1) {
            if c == '+' || c == '-' {
                split_pos = Some(i);
            }
        }

        if let Some(pos) = split_pos {
            let re_str = s[..pos].trim();
            let im_str = s[pos..].trim();

            let re = re_str
                .parse::<f64>()
                .map_err(|_| CustomError::new("invalid Complex64 real part"))?;
            let im = im_str
                .parse::<f64>()
                .map_err(|_| CustomError::new("invalid Complex64 imaginary part"))?;

            return Ok(C64(Complex64::new(re, im)));
        }
    }

    Err(CustomError::new(
        "invalid Complex64 format. Use: '3.5+2.5i', '3.5,2.5', or '{\"re\":3.5,\"im\":2.5}'",
    ))
}

type ParserFn = fn(&str) -> Result<CmTypes, CustomError>;

lazy_static! {
    /// Parsers for every explicit type
    static ref PARSERS: RapidHashMap<&'static str, ParserFn> = {
        let mut entry_map: RapidHashMap<&'static str, ParserFn> = RapidHashMap::new();
        macro_rules! add {
            ($ty:expr, $p:expr) => { entry_map.insert($ty, $p as ParserFn); };
        }
        add!("bool",    |s| s.parse::<bool>().map(CmTypes::Bool).map_err(|_| CustomError::new(&format!("invalid bool: '{}'", s))));
        add!("i8",      |s| s.parse::<i8>().map(CmTypes::I8).map_err(|_| CustomError::new(&format!("invalid i8: '{}'", s))));
        add!("i16",     |s| s.parse::<i16>().map(CmTypes::I16).map_err(|_| CustomError::new(&format!("invalid i16: '{}'", s))));
        add!("i32",     |s| s.parse::<i32>().map(CmTypes::I32).map_err(|_| CustomError::new(&format!("invalid i32: '{}'", s))));
        add!("i64",     |s| s.parse::<i64>().map(CmTypes::I64).map_err(|_| CustomError::new(&format!("invalid i64: '{}'", s))));
        add!("i128",    |s| s.parse::<i128>().map(|v| CmTypes::I128(Arc::new(v))).map_err(|_| CustomError::new(&format!("invalid i128: '{}'", s))));
        add!("u8",      |s| s.parse::<u8>().map(CmTypes::U8).map_err(|_| CustomError::new(&format!("invalid u8: '{}'", s))));
        add!("u16",     |s| s.parse::<u16>().map(CmTypes::U16).map_err(|_| CustomError::new(&format!("invalid u16: '{}'", s))));
        add!("u32",     |s| s.parse::<u32>().map(CmTypes::U32).map_err(|_| CustomError::new(&format!("invalid u32: '{}'", s))));
        add!("u64",     |s| s.parse::<u64>().map(CmTypes::U64).map_err(|_| CustomError::new(&format!("invalid u64: '{}'", s))));
        add!("u128",    |s| s.parse::<u128>().map(|v| CmTypes::U128(Arc::new(v))).map_err(|_| CustomError::new(&format!("invalid u128: '{}'", s))));
        add!("f32",     |s| s.parse::<f32>().map(CmTypes::F32).map_err(|_| CustomError::new(&format!("invalid f32: '{}'", s))));
        add!("f64",     |s| s.parse::<f64>().map(CmTypes::F64).map_err(|_| CustomError::new(&format!("invalid f64: '{}'", s))));
        add!("char",    |s| s.chars().next().map(CmTypes::Char).ok_or_else(|| CustomError::new(&format!("invalid char: '{}'", s))));
        add!("usize",   |s| s.parse::<usize>().map(CmTypes::Usize).map_err(|_| CustomError::new(&format!("invalid usize: '{}'", s))));
        add!("isize",   |s| s.parse::<isize>().map(CmTypes::Isize).map_err(|_| CustomError::new(&format!("invalid isize: '{}'", s))));
        add!("String",  |s| Ok(CmTypes::String(Arc::from(s))));
        add!("Complex32",  |s| parse_complex32(s).map(|v| CmTypes::Complex32(Arc::new(v))).map_err(|e| CustomError::new(&format!("Complex32 parse error for '{}': {}", s, e))));
        add!("Complex64",  |s| parse_complex64(s).map(|v| CmTypes::Complex64(Arc::new(v))).map_err(|e| CustomError::new(&format!("Complex64 parse error for '{}': {}", s, e))));
        add!("$ref",    |s| s.parse::<usize>().map(CmTypes::Ref).map_err(|_| CustomError::new(&format!("invalid ref: '{}'", s))));
        add!("$res",    |s| s.parse::<usize>().map(CmTypes::Res).map_err(|_| CustomError::new(&format!("invalid res: '{}'", s))));
        add!("$factor", |_| Ok(CmTypes::Ref(0)));  // Runtime node index
        add!("$worker", |_| Ok(CmTypes::Ref(1)));  // Runtime worker count
        add!("$barrier", |s| Ok(CmTypes::Barrier(Arc::from(s))));
        add!("None",    |_| Ok(CmTypes::None));
        add!("Init",    |_| Ok(CmTypes::Init));
        entry_map
    };
}

pub fn defined_type(tp: &str) -> bool {
    PARSERS.contains_key(tp)
}

pub fn string_to_cmtype(tp: String, arg: String) -> Result<CmTypes, CustomError> {
    // 1) explicit table
    if let Some(parser) = PARSERS.get(tp.as_str()) {
        return parser(&arg);
    }

    if tp.starts_with("Vec") {
        // get type inside <> markers
        let tp = tp
            .strip_prefix("Vec<")
            .and_then(|s| s.strip_suffix(">"))
            .ok_or_else(|| CustomError::new(&format!("Invalid Vec format: {}", tp)))?;

        let mut v: Vec<CmTypes> = Vec::new();
        // arg contains tp values separated by commas
        let values: Vec<&str> = arg.split(',').collect();
        for value in values {
            if let Some(parser) = PARSERS.get(tp) {
                v.push(parser(value.trim())?);
            } else {
                return Err(CustomError::new(&format!("Unable to parse type '{}'", tp)));
            }
        }
        // Return the vector of CmTypes wrapped in Arc
        Ok(CmTypes::VecCmt(Arc::new(v)))
    } else {
        // Return error
        Err(CustomError::new(&format!(
            "Unable to parse type '{}' with value '{}'",
            tp, arg
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct CustomBuffer {
        buffer: Vec<usize>,
    }

    unsafe impl Send for CustomBuffer {}
    unsafe impl Sync for CustomBuffer {}

    impl Sliceable<usize> for CustomBuffer {
        fn as_mut_slice(&mut self) -> &mut [usize] {
            &mut self.buffer
        }
    }

    #[test]
    fn test_unsafe_access() {
        let custombuf = CmTypes::from_any(CustomBuffer {
            buffer: vec![1; 10],
        });
        custombuf.with_any(|buf: &CustomBuffer| {
            println!("Init Buffer: {:?}", buf.buffer);
        });

        let ptr = unsafe { custombuf.as_mut_ptr::<CustomBuffer>().unwrap().0 as usize };

        std::thread::scope(|s| {
            for thread_index in 0..2 {
                s.spawn(move || unsafe {
                    let ptr = ptr as *mut CustomBuffer;
                    let buf = &mut *ptr;
                    let start = thread_index * (buf.buffer.len() / 2);
                    let end = start + (buf.buffer.len() / 2);
                    let add_val = thread_index * 10;
                    for i in start..end {
                        buf.buffer[i] += add_val;
                    }
                });
            }
        });

        custombuf.with_any(|buf: &CustomBuffer| {
            println!("Buffer: {:?}", buf.buffer);
        });
    }

    #[test]
    fn test_sliced_multithreaded_access() {
        let initial_data = vec![0; 10];
        let custombuf = CustomBuffer {
            buffer: initial_data.clone(),
        };

        let sliced = CmTypes::from_any_sliced(custombuf);
        let total_length = sliced.sliced_total_length().unwrap_or(0);

        println!("Total buffer length: {}", total_length);

        let sliced_ref = &sliced;
        std::thread::scope(|s| {
            for i in 0..3 {
                let range_start = i * 3;
                let range_len = 3.min(total_length - range_start);

                if range_start < total_length {
                    s.spawn(move || {
                        println!(
                            "Thread {} working on range {}-{}",
                            i,
                            range_start,
                            range_start + range_len
                        );

                        if let Some(mut mut_slice) =
                            sliced_ref.sliced_get_mut_range::<usize>(range_start, range_len)
                        {
                            let add_val = i * 10;
                            add_vals(&mut mut_slice, add_val);
                        } else {
                            println!(
                                "Thread {} could not acquire range {}-{} (already borrowed)",
                                i,
                                range_start,
                                range_start + range_len
                            );
                        }

                        // Check if range is borrowed after processing
                        if let Some(is_borrowed) =
                            sliced_ref.is_sliced_range_borrowed(range_start, range_len)
                        {
                            if is_borrowed {
                                println!(
                                    "Thread {}: Range {}-{} is still borrowed after processing",
                                    i,
                                    range_start,
                                    range_start + range_len
                                );
                            }
                        }
                    });
                }
            }
        });

        // Print final buffer state

        println!("Test completed - simplified slicing with write-bits tracking");
    }

    fn add_vals(data: &mut [usize], add_value: usize) {
        for v in data.iter_mut() {
            *v += add_value;
        }
    }

    #[test]
    fn test_cmtypes_from_any() {
        let value = 42;
        let cm_type = CmTypes::from_any(value);
        cm_type.with_any_mut(|v: &mut i32| {
            assert_eq!(*v, 42);
            *v += 1;
        });
        cm_type.with_any(|v: &i32| {
            println!("Value: {}", v);
            assert_eq!(*v, 43);
        });
    }

    #[test]
    fn test_boxed_type() {
        let value: Box<dyn Any + Send + Sync> = Box::new(42);
        let cm_type = CmTypes::from_any(value);
        cm_type.with_any(|v: &i32| {
            assert_eq!(*v, 42);
        });
    }

    #[test]
    fn test_boxed_fn() {
        let fun: Box<dyn Fn(usize) + Send + Sync> = Box::new(|value| println!("Value: {}", value));
        let cm_type = CmTypes::from_any(fun);
        cm_type.with_any(|fun: &Box<dyn Fn(usize) + Send + Sync>| {
            fun(10);
        });
    }

    #[test]
    fn test_boxed_fnmut() {
        let fun: Box<dyn FnMut(usize) + Send + Sync> =
            Box::new(|value| println!("Value: {}", value));
        let cm_type = CmTypes::from_any(fun);
        cm_type.with_any_mut(|fun: &mut Box<dyn FnMut(usize) + Send + Sync>| {
            fun(20);
        });
    }

    #[test]
    fn test_sliced_get_range() {
        // Create test data using CustomBuffer
        let test_buffer = CustomBuffer {
            buffer: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        };
        let sliced_data = CmTypes::from_any_sliced(test_buffer);

        println!("Testing sliced_get_range functionality");

        // Test 1: Multiple concurrent read accesses should work
        println!("Test 1: Multiple concurrent read accesses");
        std::thread::scope(|s| {
            for i in 0..3 {
                let data_ref = &sliced_data;
                s.spawn(move || {
                    if let Some(slice) = data_ref.sliced_get_range::<usize>(i * 2, 2) {
                        println!(
                            "Thread {} got read access to range {}-{}: {:?}",
                            i,
                            i * 2,
                            i * 2 + 1,
                            slice
                        );
                    }
                });
            }
        });

        // Test 2: Read access should be blocked when mutable access is active
        println!("Test 2: Read access blocked during mutable access");
        std::thread::scope(|s| {
            // Thread 1: Get mutable access to range 2-5
            let data_ref = &sliced_data;
            s.spawn(move || {
                if let Some(mut mut_guard) = data_ref.sliced_get_mut_range::<usize>(2, 4) {
                    println!("Thread got mutable access to range 2-5");
                    // Modify the data
                    for (i, elem) in mut_guard.iter_mut().enumerate() {
                        *elem += (i + 1) * 10;
                    }
                    // Hold the lock for a bit
                    std::thread::sleep(Duration::from_millis(50));
                    println!("Thread releasing mutable access");
                }
            });

            // Thread 2: Try to get read access to overlapping range
            let data_ref2 = &sliced_data;
            s.spawn(move || {
                std::thread::sleep(Duration::from_millis(10)); // Let first thread acquire lock

                // This should fail because of mutable borrow
                if data_ref2.sliced_get_range::<usize>(3, 2).is_some() {
                    println!("ERROR: Read access succeeded during mutable borrow!");
                } else {
                    println!("Read access correctly blocked during mutable borrow");
                }

                // Wait for mutable borrow to be released, then try again
                std::thread::sleep(Duration::from_millis(50));
                if let Some(slice) = data_ref2.sliced_get_range::<usize>(3, 2) {
                    println!(
                        "Read access succeeded after mutable borrow released: {:?}",
                        slice
                    );
                }
            });
        });

        // Test 3: Test timeout functionality
        println!("Test 3: Testing timeout functionality");
        let timeout_result =
            sliced_data.sliced_get_range_timeout::<usize>(0, 3, Duration::from_millis(10));
        if let Some(slice) = timeout_result {
            println!("Got slice with timeout: {:?}", slice);
        }

        // Test 4: Test range checking
        println!("Test 4: Testing range checking");
        println!("Can read range 0-5: {}", sliced_data.can_read_range(0, 5));
        println!(
            "Can read range 8-3 (out of bounds): {}",
            sliced_data.can_read_range(8, 3)
        );

        // Test 5: Verify data was modified by the mutable access
        println!("Test 5: Verifying data modifications");
        if let Some(slice) = sliced_data.sliced_get_range::<usize>(0, 10) {
            println!("Final data after modifications: {:?}", slice);
        }
    }

    #[test]
    fn test_complex_serialization() {
        use num_complex::{Complex32, Complex64};

        // Test Complex32
        let c32 = C32(Complex32::new(3.5, 2.5));
        let json = serde_json::to_string(&c32).unwrap();
        println!("Complex32 serialized: {}", json);
        let deserialized: C32 = serde_json::from_str(&json).unwrap();
        assert_eq!(c32, deserialized);

        // Test Complex64
        let c64 = C64(Complex64::new(7.25, -4.75));
        let json = serde_json::to_string(&c64).unwrap();
        println!("Complex64 serialized: {}", json);
        let deserialized: C64 = serde_json::from_str(&json).unwrap();
        assert_eq!(c64, deserialized);

        // Test in CmTypes enum
        let cm_c32 = CmTypes::Complex32(Arc::new(C32(Complex32::new(1.0, 2.0))));
        let cm_c64 = CmTypes::Complex64(Arc::new(C64(Complex64::new(3.0, 4.0))));

        println!("CmTypes::Complex32 display: {}", cm_c32);
        println!("CmTypes::Complex64 display: {}", cm_c64);
        println!("CmTypes::Complex32 debug: {:?}", cm_c32);
        println!("CmTypes::Complex64 debug: {:?}", cm_c64);
    }

    #[test]
    fn test_complex_parsing() {
        // Test Complex32 parsing with different formats

        // Standard format: a+bi
        let result = string_to_cmtype("Complex32".to_string(), "3.5+2.5i".to_string());
        assert!(result.is_ok());
        if let Ok(CmTypes::Complex32(c)) = result {
            assert_eq!(c.0.re, 3.5);
            assert_eq!(c.0.im, 2.5);
            println!("Parsed '3.5+2.5i' as Complex32: {}", CmTypes::Complex32(c));
        }

        // Standard format with negative imaginary: a-bi
        let result = string_to_cmtype("Complex32".to_string(), "3.5-2.5i".to_string());
        assert!(result.is_ok());
        if let Ok(CmTypes::Complex32(c)) = result {
            assert_eq!(c.0.re, 3.5);
            assert_eq!(c.0.im, -2.5);
            println!("Parsed '3.5-2.5i' as Complex32: {}", CmTypes::Complex32(c));
        }

        // Comma-separated format
        let result = string_to_cmtype("Complex32".to_string(), "1.5,4.5".to_string());
        assert!(result.is_ok());
        if let Ok(CmTypes::Complex32(c)) = result {
            assert_eq!(c.0.re, 1.5);
            assert_eq!(c.0.im, 4.5);
            println!("Parsed '1.5,4.5' as Complex32: {}", CmTypes::Complex32(c));
        }

        // JSON format
        let result = string_to_cmtype(
            "Complex32".to_string(),
            r#"{"re":7.5,"im":-3.5}"#.to_string(),
        );
        assert!(result.is_ok());
        if let Ok(CmTypes::Complex32(c)) = result {
            assert_eq!(c.0.re, 7.5);
            assert_eq!(c.0.im, -3.5);
            println!("Parsed JSON as Complex32: {}", CmTypes::Complex32(c));
        }

        // Test Complex64 parsing with different formats

        // Standard format: a+bi
        let result = string_to_cmtype("Complex64".to_string(), "10.5+20.5i".to_string());
        assert!(result.is_ok());
        if let Ok(CmTypes::Complex64(c)) = result {
            assert_eq!(c.0.re, 10.5);
            assert_eq!(c.0.im, 20.5);
            println!(
                "Parsed '10.5+20.5i' as Complex64: {}",
                CmTypes::Complex64(c)
            );
        }

        // Test that defined_type recognizes Complex32 and Complex64
        assert!(defined_type("Complex32"));
        assert!(defined_type("Complex64"));

        println!("All complex parsing tests passed!");
    }
}
