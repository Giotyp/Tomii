use crossbeam_utils::Backoff;
use parking_lot::{Condvar, Mutex};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The Node represents a single packet/item in the lock-free stack.
struct Node<T> {
    data: T,
    next: *mut Node<T>,
}

#[derive(Debug)]
struct Inner<T> {
    head: AtomicPtr<Node<T>>,
    receiver_alive: AtomicBool,
    /// Lazy-notification flag.  Senders only call `notify_one` on the
    /// false→true transition, collapsing N consecutive pushes into a single
    /// futex wake when the receiver is actively draining.  Receivers clear
    /// the flag immediately before parking on the condvar so that any
    /// subsequent push will re-arm the notification.
    has_items: AtomicBool,
    // The lock is only used for the Condvar, not for the data transfer.
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

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        head: AtomicPtr::new(ptr::null_mut()),
        receiver_alive: AtomicBool::new(true),
        has_items: AtomicBool::new(false),
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
    /// Lock-free send. Returns Err(item) if the receiver is dropped.
    pub fn try_send(&self, item: T) -> Result<(), T> {
        if !self.inner.receiver_alive.load(Ordering::Acquire) {
            return Err(item);
        }

        let new_node = Box::into_raw(Box::new(Node {
            data: item,
            next: ptr::null_mut(),
        }));

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
                // Lazy notification: only wake the receiver on the false→true
                // transition.  When the receiver is actively draining (flag is
                // already true) we skip the futex entirely.
                if !self.inner.has_items.swap(true, Ordering::Release) {
                    self.inner.condvar.notify_one();
                }
                return Ok(());
            }
        }
    }
}

impl<T> Receiver<T> {
    /// NON-BLOCKING: Check if queue is empty without consuming items
    pub fn is_empty(&self) -> bool {
        self.inner.head.load(Ordering::Relaxed).is_null()
    }

    /// NON-BLOCKING: Returns up to max_items from the queue.
    /// Allows multiple threads to drain chunks in parallel.
    /// Returns an empty Vec if no items are present.
    pub fn try_recv_chunk(&self, max_items: usize) -> Vec<T> {
        if max_items == 0 {
            return Vec::new();
        }

        let mut batch = Vec::with_capacity(max_items);

        // Backoff for fair CAS contention handling
        let backoff = Backoff::new();

        // Atomically extract up to max_items using CAS loop with backoff
        loop {
            let current_head = self.inner.head.load(Ordering::Acquire);
            if current_head.is_null() {
                break; // Queue empty
            }

            // Walk the chain to find the new head (after extracting max_items)
            let mut count = 0;
            let mut last_node = current_head;
            let mut temp = current_head;

            unsafe {
                while !temp.is_null() && count < max_items {
                    last_node = temp;
                    temp = (*temp).next;
                    count += 1;
                }
            }

            if count == 0 {
                break; // Race: another thread drained the queue
            }

            // Try to atomically update head to skip the extracted nodes
            let new_head = unsafe { (*last_node).next };
            if self
                .inner
                .head
                .compare_exchange_weak(current_head, new_head, Ordering::Release, Ordering::Acquire)
                .is_ok()
            {
                // Successfully claimed this chunk - extract the data
                let mut node = current_head;
                while node != new_head {
                    unsafe {
                        let boxed = Box::from_raw(node);
                        batch.push(boxed.data);
                        node = boxed.next;
                    }
                }
                break;
            }

            // CAS failed - back off to give other threads a chance
            // This prevents one thread from monopolizing the queue in tight CAS loops
            backoff.spin();
        }

        // Treiber stacks are LIFO; we reverse to maintain FIFO packet order.
        batch.reverse();
        batch
    }

    /// NON-BLOCKING: Returns all items currently in the queue.
    /// Returns an empty Vec if no items are present.
    pub fn try_recv_all(&self) -> Vec<T> {
        let mut head = self.inner.head.swap(ptr::null_mut(), Ordering::Acquire);
        if head.is_null() {
            return Vec::new();
        }

        let mut batch = Vec::new();
        while !head.is_null() {
            unsafe {
                let node = Box::from_raw(head);
                batch.push(node.data);
                head = node.next;
            }
        }
        // Treiber stacks are LIFO; we reverse to maintain FIFO packet order.
        batch.reverse();
        batch
    }

    /// BLOCKING: Wait for and return a single item (Option for graceful shutdown)
    pub fn recv(&self) -> Option<T> {
        let backoff = Backoff::new();

        // Phase 1: Adaptive Spinning
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return None;
            }
            if backoff.is_completed() {
                break;
            }
            backoff.spin();
        }

        // Phase 2: Condvar Wait
        // Clear flag before parking so any sender that pushes after this
        // point will see false, set true, and call notify_one().
        self.inner.has_items.store(false, Ordering::Release);
        let mut lock = self.inner.cv_lock.lock();
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return None;
            }
            self.inner.condvar.wait(&mut lock);
        }
        drop(lock);

        // Phase 3: Extract single item using CAS
        loop {
            let current_head = self.inner.head.load(Ordering::Acquire);
            if current_head.is_null() {
                // Race condition: another thread drained the queue
                return self.recv(); // Retry
            }

            unsafe {
                let next = (*current_head).next;
                if self
                    .inner
                    .head
                    .compare_exchange(current_head, next, Ordering::Release, Ordering::Acquire)
                    .is_ok()
                {
                    let node = Box::from_raw(current_head);
                    return Some(node.data);
                }
            }
        }
    }

    /// BLOCKING: Spins briefly, then parks the thread until at least one item arrives.
    /// Returns all items that accumulated during the wait.
    pub fn recv_all(&self) -> Vec<T> {
        let backoff = Backoff::new();

        // Phase 1: Adaptive Spinning (User-space)
        // This is much faster than a manual while loop because it uses the
        // PAUSE instruction to avoid CPU pipeline stalls.
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return Vec::new();
            }
            if backoff.is_completed() {
                break;
            }
            backoff.spin();
        }

        // Phase 2: Condvar Wait (Kernel-space)
        // We only hit this if the spin phase fails (i.e., the queue is truly idle).
        // Clear flag before parking — any sender pushing after this will re-arm.
        self.inner.has_items.store(false, Ordering::Release);
        let mut lock = self.inner.cv_lock.lock();
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return Vec::new();
            }
            self.inner.condvar.wait(&mut lock);
        }
        drop(lock);

        self.try_recv_all()
    }

    /// BLOCKING WITH TIMEOUT: Useful for flushing buffers periodically.
    pub fn recv_timeout_all(&self, timeout: Duration) -> Vec<T> {
        // Fast path: non-blocking drain
        let items = self.try_recv_all();
        if !items.is_empty() {
            return items;
        }

        // Queue is empty — clear the flag before sleeping.  Any sender that
        // pushes after this point will see false and call notify_one().
        self.inner.has_items.store(false, Ordering::Release);

        // Re-check: a sender may have pushed between our drain and the flag clear.
        let items = self.try_recv_all();
        if !items.is_empty() {
            return items;
        }

        // Genuinely empty — sleep with timeout.
        let mut lock = self.inner.cv_lock.lock();
        self.inner.condvar.wait_for(&mut lock, timeout);
        drop(lock);

        self.try_recv_all()
    }

    /// BLOCKING WITH TIMEOUT: Returns up to max_items, waiting up to timeout duration.
    /// This is the optimal method for batched processing with bounded latency:
    /// - If items available: returns immediately (up to max_items)
    /// - If empty: waits up to timeout for items to arrive, then returns what's available
    ///
    /// Use this for resolution threads that want to process batches with configurable
    /// size and latency bounds (controlled by batching_size and batching_limit args).
    pub fn try_recv_chunk_timeout(&self, max_items: usize, timeout: Duration) -> Vec<T> {
        if max_items == 0 {
            return Vec::new();
        }

        // Phase 1: Try immediate non-blocking pull
        let batch = self.try_recv_chunk(max_items);
        if !batch.is_empty() {
            return batch;
        }

        // Phase 2: Clear flag before sleeping so the first subsequent push
        // will re-arm the notification.
        self.inner.has_items.store(false, Ordering::Release);

        // Re-check: a sender may have pushed between Phase 1 and the flag clear.
        let batch = self.try_recv_chunk(max_items);
        if !batch.is_empty() {
            return batch;
        }

        // Phase 3: Wait with timeout for items to arrive
        let mut lock = self.inner.cv_lock.lock();
        #[allow(clippy::never_loop)] // Intentional: condvar wait pattern with early returns
        while self.inner.head.load(Ordering::Relaxed).is_null() {
            if !self.inner.receiver_alive.load(Ordering::Acquire) {
                return Vec::new();
            }
            if self.inner.condvar.wait_for(&mut lock, timeout).timed_out() {
                return Vec::new();
            }
            break;
        }
        drop(lock);

        // Phase 4: Pull available items (up to max_items)
        self.try_recv_chunk(max_items)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // 1. Mark the channel as dead so Senders stop pushing
        self.inner.receiver_alive.store(false, Ordering::Release);

        // 2. Wake up any threads potentially parked on the condvar
        // (Though in our specific MPMC design, usually only the receiver parks here)
        self.inner.condvar.notify_all();

        // 3. CRITICAL: Memory Cleanup
        // Since we used Box::into_raw, we must manually reclaim the memory
        // to prevent leaks of any items left in the queue.
        let mut head = self
            .inner
            .head
            .swap(std::ptr::null_mut(), Ordering::Acquire);
        while !head.is_null() {
            unsafe {
                let node = Box::from_raw(head);
                // node goes out of scope here, dropping T and the Node itself
                head = node.next;
            }
        }
    }
}
