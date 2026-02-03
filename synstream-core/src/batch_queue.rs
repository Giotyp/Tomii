use crossbeam_utils::{Backoff, CachePadded};
use parking_lot::{Condvar, Mutex};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The Node structure used for both the Inbox and the Freelist.
struct Node<T> {
    // We use Option to allow taking data out while recycling the node.
    data: Option<T>,
    next: *mut Node<T>,
}

/// A Lock-Free Freelist to recycle Node allocations.
#[derive(Debug)]
struct NodePool<T> {
    free_list: AtomicPtr<Node<T>>,
}

impl<T> NodePool<T> {
    fn new(initial_capacity: usize) -> Self {
        let pool = Self {
            free_list: AtomicPtr::new(ptr::null_mut()),
        };
        for _ in 0..initial_capacity {
            let node = Box::into_raw(Box::new(Node {
                data: None,
                next: ptr::null_mut(),
            }));
            pool.push(node);
        }
        pool
    }

    fn pop(&self) -> *mut Node<T> {
        let backoff = Backoff::new();
        loop {
            let head = self.free_list.load(Ordering::Acquire);
            if head.is_null() {
                // Fallback: Expand pool if exhausted
                return Box::into_raw(Box::new(Node {
                    data: None,
                    next: ptr::null_mut(),
                }));
            }
            let next = unsafe { (*head).next };
            if self
                .free_list
                .compare_exchange_weak(head, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return head;
            }
            backoff.spin();
        }
    }

    fn push(&self, node: *mut Node<T>) {
        let backoff = Backoff::new();
        loop {
            let head = self.free_list.load(Ordering::Relaxed);
            unsafe {
                (*node).next = head;
            }
            if self
                .free_list
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
            backoff.spin();
        }
    }
}

#[derive(Debug)]
struct Inner<T> {
    // Padded to prevent False Sharing between producers (head) and receiver (has_items)
    head: CachePadded<AtomicPtr<Node<T>>>,
    has_items: CachePadded<AtomicBool>,

    receiver_alive: AtomicBool,
    pool: NodePool<T>,

    cv_lock: Mutex<()>,
    condvar: Condvar,
}

#[derive(Debug, Clone)]
pub struct Sender<T> {
    inner: Arc<Inner<T>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Arc<Inner<T>>,
}

/// Create an unbounded batch queue with a pre-allocated pool of 1024 nodes.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        head: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
        has_items: CachePadded::new(AtomicBool::new(false)),
        receiver_alive: AtomicBool::new(true),
        pool: NodePool::new(1024),
        cv_lock: Mutex::new(()),
        condvar: Condvar::new(),
    });

    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    pub fn try_send(&self, item: T) -> Result<(), T> {
        if !self.inner.receiver_alive.load(Ordering::Acquire) {
            return Err(item);
        }

        // Fast Path: Get node from Pool instead of Global Heap
        let new_node = self.inner.pool.pop();
        unsafe {
            (*new_node).data = Some(item);
        }

        loop {
            let current_head = self.inner.head.load(Ordering::Relaxed);
            unsafe {
                (*new_node).next = current_head;
            }

            if self
                .inner
                .head
                .compare_exchange_weak(current_head, new_node, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                // Lazy-notify: Only wake if the receiver isn't already active
                if !self.inner.has_items.swap(true, Ordering::Release) {
                    self.inner.condvar.notify_all();
                }
                return Ok(());
            }
        }
    }
}

impl<T> Receiver<T> {
    /// Internal helper: Swaps the list and reverses it in-place to achieve FIFO.
    /// Reversing in-place is $O(N)$ but avoids the $O(N)$ data movement of Vec::reverse.
    fn drain_to_vec(&self) -> Vec<T> {
        let mut curr = self.inner.head.swap(ptr::null_mut(), Ordering::Acquire);
        if curr.is_null() {
            return Vec::new();
        }

        // 1. In-place reversal of the linked list (LIFO -> FIFO)
        let mut prev = ptr::null_mut();
        let mut count = 0;
        while !curr.is_null() {
            let next = unsafe { (*curr).next };
            unsafe {
                (*curr).next = prev;
            }
            prev = curr;
            curr = next;
            count += 1;
        }

        // 2. Transfer data and recycle nodes to pool
        let mut batch = Vec::with_capacity(count);
        let mut node_ptr = prev;
        while !node_ptr.is_null() {
            unsafe {
                let node = &mut *node_ptr;
                let next = node.next;
                if let Some(val) = node.data.take() {
                    batch.push(val);
                }
                self.inner.pool.push(node_ptr);
                node_ptr = next;
            }
        }
        batch
    }

    pub fn try_recv_all(&self) -> Vec<T> {
        self.drain_to_vec()
    }

    pub fn recv_all(&self) -> Vec<T> {
        let backoff = Backoff::new();
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return Vec::new();
            }
            if backoff.is_completed() {
                break;
            }
            backoff.spin();
        }

        // Fix: Lock BEFORE clearing has_items to prevent lost wakeup
        let mut lock = self.inner.cv_lock.lock();
        self.inner.has_items.store(false, Ordering::Release);
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return Vec::new();
            }
            self.inner.condvar.wait(&mut lock);
        }
        drop(lock);

        self.drain_to_vec()
    }

    pub fn recv_timeout_all(&self, timeout: Duration) -> Vec<T> {
        let items = self.try_recv_all();
        if !items.is_empty() {
            return items;
        }

        // Fix: Lock BEFORE clearing has_items to prevent lost wakeup
        let mut lock = self.inner.cv_lock.lock();
        self.inner.has_items.store(false, Ordering::Release);
        if self.inner.head.load(Ordering::Relaxed).is_null() {
            self.inner.condvar.wait_for(&mut lock, timeout);
        }
        drop(lock);

        self.drain_to_vec()
    }

    pub fn recv_chunk_timeout(&self, max_items: usize, timeout: Duration) -> Vec<T> {
        if max_items == 0 {
            return Vec::new();
        }

        // 1. Phase 1: Try immediate non-blocking pull
        let batch = self.internal_try_recv_chunk(max_items);
        if !batch.is_empty() {
            return batch;
        }

        // 2. Phase 2: Lock and prepare for park
        // Fix: Lock BEFORE clearing has_items to prevent lost wakeup race condition.
        // This ensures atomicity between flag clearing, queue checking, and waiting.
        let mut lock = self.inner.cv_lock.lock();
        self.inner.has_items.store(false, Ordering::Release);

        // Re-check queue while holding lock (prevents race with senders)
        if !self.inner.head.load(Ordering::Relaxed).is_null() {
            drop(lock);
            return self.internal_try_recv_chunk(max_items);
        }

        // 3. Phase 3: Wait with timeout (releases lock atomically)
        if self.inner.condvar.wait_for(&mut lock, timeout).timed_out() {
            drop(lock);
            return Vec::new();
        }
        drop(lock);

        // 4. Phase 4: Final pull attempt
        self.internal_try_recv_chunk(max_items)
    }

    /// Internal helper to "snip" a chunk from the lock-free stack
    fn internal_try_recv_chunk(&self, max_items: usize) -> Vec<T> {
        let backoff = Backoff::new();
        loop {
            let current_head = self.inner.head.load(Ordering::Acquire);
            if current_head.is_null() {
                return Vec::new();
            }

            let mut count = 0;
            let mut end_node = current_head;

            // Walk the chain to find the split point
            unsafe {
                while count < max_items - 1 {
                    let next = (*end_node).next;
                    if next.is_null() {
                        break;
                    }
                    end_node = next;
                    count += 1;
                }
            }

            // Attempt to "snip" this segment from the main list
            let next_head = unsafe { (*end_node).next };
            if self
                .inner
                .head
                .compare_exchange_weak(
                    current_head,
                    next_head,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // Successfully claimed [current_head ... end_node]
                unsafe {
                    (*end_node).next = ptr::null_mut();
                }
                return self.process_segment_to_fifo(current_head);
            }
            backoff.spin();
        }
    }

    /// Helper to reverse the snipped segment and recycle nodes
    fn process_segment_to_fifo(&self, mut curr: *mut Node<T>) -> Vec<T> {
        let mut prev = ptr::null_mut();
        let mut count = 0;

        // In-place reversal of the segment
        while !curr.is_null() {
            let next = unsafe { (*curr).next };
            unsafe {
                (*curr).next = prev;
            }
            prev = curr;
            curr = next;
            count += 1;
        }

        let mut batch = Vec::with_capacity(count);
        let mut node_ptr = prev;
        while !node_ptr.is_null() {
            unsafe {
                let node = &mut *node_ptr;
                let next = node.next;
                if let Some(val) = node.data.take() {
                    batch.push(val);
                }
                self.inner.pool.push(node_ptr);
                node_ptr = next;
            }
        }
        batch
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.inner.receiver_alive.store(false, Ordering::Release);
        self.inner.condvar.notify_all();

        // Final cleanup of the Inbox
        let mut head = self.inner.head.swap(ptr::null_mut(), Ordering::Acquire);
        while !head.is_null() {
            unsafe {
                let node = Box::from_raw(head);
                head = node.next;
                // node/T dropped here
            }
        }

        // Final cleanup of the Freelist
        let mut free = self
            .inner
            .pool
            .free_list
            .swap(ptr::null_mut(), Ordering::Acquire);
        while !free.is_null() {
            unsafe {
                let node = Box::from_raw(free);
                free = node.next;
            }
        }
    }
}
