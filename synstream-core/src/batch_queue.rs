use crossbeam_utils::{Backoff, CachePadded};
use parking_lot::{Condvar, Mutex};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The Node structure used for the lock-free Treiber stack.
struct Node<T> {
    data: Option<T>,
    next: *mut Node<T>,
}

#[derive(Debug)]
struct Inner<T> {
    // Padded to prevent False Sharing between producers (head) and receiver (has_items)
    head: CachePadded<AtomicPtr<Node<T>>>,
    has_items: CachePadded<AtomicBool>,

    receiver_alive: AtomicBool,

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

/// Create an unbounded batch queue.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        head: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
        has_items: CachePadded::new(AtomicBool::new(false)),
        receiver_alive: AtomicBool::new(true),
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

        // Allocate a fresh node for each send to avoid ABA problem with node recycling
        let new_node = Box::into_raw(Box::new(Node {
            data: Some(item),
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

        // 2. Transfer data and deallocate nodes
        let mut batch = Vec::with_capacity(count);
        let mut node_ptr = prev;
        while !node_ptr.is_null() {
            unsafe {
                let next = (*node_ptr).next;
                if let Some(val) = (*node_ptr).data.take() {
                    batch.push(val);
                }
                drop(Box::from_raw(node_ptr));
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
        // Use atomic-swap drain (safe for multiple consumers), but respect max_items
        // to enable fair work distribution across multiple resolution threads.
        // If we get more than max_items, re-inject the excess back into the queue.
        let batch = self.try_recv_all();
        if !batch.is_empty() {
            return self.limit_and_requeue(batch, max_items);
        }

        // Lock BEFORE clearing has_items to prevent lost wakeup race condition.
        let mut lock = self.inner.cv_lock.lock();
        self.inner.has_items.store(false, Ordering::Release);

        // Re-check queue while holding lock (prevents race with senders)
        if !self.inner.head.load(Ordering::Relaxed).is_null() {
            drop(lock);
            let batch = self.try_recv_all();
            return self.limit_and_requeue(batch, max_items);
        }

        // Wait with timeout (releases lock atomically)
        if self.inner.condvar.wait_for(&mut lock, timeout).timed_out() {
            drop(lock);
            return Vec::new();
        }
        drop(lock);

        let batch = self.try_recv_all();
        self.limit_and_requeue(batch, max_items)
    }

    /// Helper: Limit batch to max_items and re-inject excess back into queue.
    /// This enables fair work distribution without CAS-based list walking.
    fn limit_and_requeue(&self, mut batch: Vec<T>, max_items: usize) -> Vec<T> {
        if batch.len() <= max_items {
            return batch;
        }

        // Take only max_items, put the rest back
        let excess: Vec<T> = batch.drain(max_items..).collect();

        // Re-inject excess items back into the queue for other consumers
        // Using reverse order to maintain original FIFO ordering
        for item in excess.into_iter().rev() {
            // Allocate a fresh node (safe, no ABA problem)
            let new_node = Box::into_raw(Box::new(Node {
                data: Some(item),
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
                    break;
                }
            }
        }

        // Wake other threads since we just put items back
        if !self.inner.has_items.swap(true, Ordering::Release) {
            self.inner.condvar.notify_all();
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
    }
}
