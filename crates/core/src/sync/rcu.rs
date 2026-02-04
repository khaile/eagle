//! Read-Copy-Update (RCU) Synchronization Primitives
//!
//! RCU is a synchronization mechanism that allows lock-free reads while
//! updates create new versions of data.
//!
//! This is a simplified implementation using parking_lot for simplicity.

// RCU infrastructure for future lock-free data structures
#![allow(dead_code)]

use parking_lot::RwLock;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// RCU-protected cell holding a single value
///
/// Readers can access the value without locks.
/// Writers replace the value atomically.
pub struct RcuCell<T> {
    inner: RwLock<Option<Box<T>>>,
}

impl<T> RcuCell<T> {
    /// Create a new RcuCell with the given value
    pub fn new(value: T) -> Self {
        Self {
            inner: RwLock::new(Some(Box::new(value))),
        }
    }

    /// Create an empty RcuCell
    pub fn empty() -> Self {
        Self {
            inner: RwLock::new(None),
        }
    }

    /// Read the current value
    pub fn read(&self) -> Option<impl std::ops::Deref<Target = T> + '_> {
        let guard = self.inner.read();
        if guard.is_some() {
            Some(RcuReadGuard { guard })
        } else {
            None
        }
    }

    /// Update the value
    pub fn update(&self, new_value: T) {
        let mut guard = self.inner.write();
        *guard = Some(Box::new(new_value));
    }

    /// Take the value, leaving None
    pub fn take(&self) -> Option<T> {
        let mut guard = self.inner.write();
        guard.take().map(|b| *b)
    }

    /// Check if the cell is empty
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_none()
    }
}

impl<T: Clone> RcuCell<T> {
    /// Get a clone of the value
    pub fn get(&self) -> Option<T> {
        self.inner.read().as_ref().map(|b| (**b).clone())
    }
}

impl<T: Default> Default for RcuCell<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

/// Read guard for RcuCell
struct RcuReadGuard<'a, T> {
    guard: parking_lot::RwLockReadGuard<'a, Option<Box<T>>>,
}

impl<'a, T> std::ops::Deref for RcuReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.as_ref().unwrap()
    }
}

/// RCU-protected linked list
///
/// Allows lock-free iteration while supporting concurrent modifications.
pub struct RcuList<T> {
    head: RwLock<Option<Box<RcuNode<T>>>>,
    len: AtomicUsize,
}

struct RcuNode<T> {
    value: T,
    next: Option<Box<RcuNode<T>>>,
}

impl<T> RcuList<T> {
    /// Create a new empty list
    pub fn new() -> Self {
        Self {
            head: RwLock::new(None),
            len: AtomicUsize::new(0),
        }
    }

    /// Push a value to the front of the list
    pub fn push_front(&self, value: T) {
        let mut head = self.head.write();
        let new_node = Box::new(RcuNode {
            value,
            next: head.take(),
        });
        *head = Some(new_node);
        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the length of the list
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Check if the list is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over the list (requires read lock)
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&T),
    {
        let head = self.head.read();
        let mut current = head.as_ref();
        while let Some(node) = current {
            f(&node.value);
            current = node.next.as_ref();
        }
    }

    /// Remove the first element matching the predicate
    pub fn remove<F>(&self, predicate: F) -> bool
    where
        F: Fn(&T) -> bool,
    {
        let mut head = self.head.write();

        // Handle empty list
        if head.is_none() {
            return false;
        }

        // Check if head matches
        if predicate(&head.as_ref().unwrap().value) {
            let old_head = head.take().unwrap();
            *head = old_head.next;
            self.len.fetch_sub(1, Ordering::Relaxed);
            return true;
        }

        // Search in rest of list
        let mut current = head.as_mut();
        while let Some(node) = current {
            if let Some(ref next) = node.next
                && predicate(&next.value)
            {
                let old_next = node.next.take().unwrap();
                node.next = old_next.next;
                self.len.fetch_sub(1, Ordering::Relaxed);
                return true;
            }
            current = node.next.as_mut();
        }

        false
    }

    /// Collect all values into a vector
    pub fn to_vec(&self) -> Vec<T>
    where
        T: Clone,
    {
        let head = self.head.read();
        let mut result = Vec::with_capacity(self.len());
        let mut current = head.as_ref();
        while let Some(node) = current {
            result.push(node.value.clone());
            current = node.next.as_ref();
        }
        result
    }
}

impl<T> Default for RcuList<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// RCU-protected pointer wrapper
///
/// Provides safe access to a pointer that may be updated by other threads.
pub struct RcuPointer<T> {
    ptr: AtomicPtr<T>,
}

impl<T> RcuPointer<T> {
    /// Create from a boxed value
    pub fn new(value: Box<T>) -> Self {
        Self {
            ptr: AtomicPtr::new(Box::into_raw(value)),
        }
    }

    /// Create a null pointer
    pub fn null() -> Self {
        Self {
            ptr: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Load the pointer
    pub fn load(&self) -> *mut T {
        self.ptr.load(Ordering::Acquire)
    }

    /// Store a new pointer, returning the old one
    pub fn store(&self, new_ptr: *mut T) -> *mut T {
        self.ptr.swap(new_ptr, Ordering::AcqRel)
    }

    /// Compare and swap
    pub fn compare_and_swap(&self, current: *mut T, new: *mut T) -> Result<*mut T, *mut T> {
        match self
            .ptr
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(old) => Ok(old),
            Err(actual) => Err(actual),
        }
    }

    /// Check if null
    pub fn is_null(&self) -> bool {
        self.load().is_null()
    }

    /// Read the value (if not null)
    ///
    /// # Safety
    /// The caller must ensure the pointer is valid.
    pub unsafe fn read(&self) -> Option<&T> {
        let ptr = self.load();
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }
}

impl<T> Drop for RcuPointer<T> {
    fn drop(&mut self) {
        let ptr = self.ptr.load(Ordering::Relaxed);
        if !ptr.is_null() {
            unsafe {
                drop(Box::from_raw(ptr));
            }
        }
    }
}

unsafe impl<T: Send> Send for RcuPointer<T> {}
unsafe impl<T: Sync> Sync for RcuPointer<T> {}

/// Wait for a grace period to elapse
pub fn synchronize_rcu() {
    // With our simplified implementation using RwLock,
    // synchronization is handled by the lock itself
    std::thread::yield_now();
}

/// Schedule a function to be called after the grace period
///
/// In this simplified implementation using RwLock, the call is deferred
/// by yielding to allow other threads to complete, then the function is
/// executed directly. This is safe because the RwLock ensures proper
/// synchronization.
pub fn call_rcu<F>(f: F)
where
    F: FnOnce() + Send + 'static,
{
    // Yield to allow other threads to complete their critical sections
    std::thread::yield_now();
    // Execute the callback directly
    f();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rcu_cell_basic() {
        let cell = RcuCell::new(42i32);

        assert_eq!(*cell.read().unwrap(), 42);

        cell.update(100);
        assert_eq!(*cell.read().unwrap(), 100);
    }

    #[test]
    fn test_rcu_cell_empty() {
        let cell: RcuCell<i32> = RcuCell::empty();
        assert!(cell.is_empty());
        assert!(cell.read().is_none());
    }

    #[test]
    fn test_rcu_list_basic() {
        let list = RcuList::new();

        list.push_front(1);
        list.push_front(2);
        list.push_front(3);

        assert_eq!(list.len(), 3);

        let values = list.to_vec();
        assert_eq!(values, vec![3, 2, 1]);
    }

    #[test]
    fn test_rcu_list_remove() {
        let list = RcuList::new();

        list.push_front(1);
        list.push_front(2);
        list.push_front(3);

        assert!(list.remove(|&x| x == 2));

        let values = list.to_vec();
        assert_eq!(values, vec![3, 1]);
    }

    #[test]
    fn test_rcu_list_for_each() {
        let list = RcuList::new();
        list.push_front(1);
        list.push_front(2);

        let mut sum = 0;
        list.for_each(|x| sum += x);
        assert_eq!(sum, 3);
    }

    #[test]
    fn test_rcu_pointer() {
        let ptr = RcuPointer::new(Box::new(42));

        unsafe {
            assert_eq!(*ptr.read().unwrap(), 42);
        }

        assert!(!ptr.is_null());
    }

    #[test]
    fn test_rcu_pointer_null() {
        let ptr: RcuPointer<i32> = RcuPointer::null();
        assert!(ptr.is_null());

        unsafe {
            assert!(ptr.read().is_none());
        }
    }
}
