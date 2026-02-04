# Concurrency and Thread Safety

## Thread Basics

### Spawning Threads

```rust
use std::thread;

// Basic spawn
let handle = thread::spawn(|| {
    println!("Hello from thread!");
});

handle.join().unwrap();  // Wait for thread to finish

// With move closure (transfer ownership)
let data = vec![1, 2, 3];
let handle = thread::spawn(move || {
    println!("{:?}", data);
});
```

### Scoped Threads

Allow borrowing non-`'static` data:

```rust
let mut data = vec![1, 2, 3];

thread::scope(|s| {
    s.spawn(|| {
        println!("Length: {}", data.len());
    });

    s.spawn(|| {
        data.push(4);  // Can mutate if no other borrows
    });
});  // All threads joined automatically
```

## Thread Safety Traits

### `Send` - Can Transfer Between Threads

```rust
// Send: Ownership can be transferred to another thread
fn send_to_thread<T: Send>(value: T) {
    thread::spawn(move || {
        drop(value);
    });
}

// Examples:
// - Vec<T> is Send if T is Send
// - Arc<T> is Send if T is Send + Sync
// - Rc<T> is NOT Send (not thread-safe)
```

### `Sync` - Can Share Between Threads

```rust
// Sync: Safe to reference from multiple threads
// T is Sync if &T is Send

// Examples:
// - i32 is Sync
// - Mutex<T> is Sync if T is Send
// - Cell<T> is NOT Sync (interior mutability without sync)
// - RefCell<T> is NOT Sync
```

### Auto Traits

Both `Send` and `Sync` are auto-implemented based on fields:

```rust
// Automatically Send + Sync if all fields are
struct Point {
    x: i32,
    y: i32,
}

// Opt out with PhantomData
use std::marker::PhantomData;
use std::cell::Cell;

struct NotSync {
    data: i32,
    _marker: PhantomData<Cell<()>>,  // Removes Sync
}

// Manual implementation (unsafe)
unsafe impl Send for MyType {}
unsafe impl Sync for MyType {}
```

## Shared Ownership

### `Arc<T>` - Atomic Reference Counting

Thread-safe reference counting:

```rust
use std::sync::Arc;

let data = Arc::new(vec![1, 2, 3]);
let data_clone = data.clone();

thread::spawn(move || {
    println!("{:?}", data_clone);
});

println!("{:?}", data);  // Original still valid
```

### `Rc<T>` vs `Arc<T>`

```rust
// Rc: Single-threaded, faster, NOT Send
use std::rc::Rc;
let rc = Rc::new(5);

// Arc: Multi-threaded, slower (atomic ops), IS Send
use std::sync::Arc;
let arc = Arc::new(5);
```

## Interior Mutability for Concurrency

### `Mutex<T>` - Mutual Exclusion Lock

```rust
use std::sync::Mutex;

let counter = Mutex::new(0);

thread::scope(|s| {
    for _ in 0..10 {
        s.spawn(|| {
            let mut num = counter.lock().unwrap();
            *num += 1;
        });  // Lock released when guard dropped
    }
});

assert_eq!(*counter.lock().unwrap(), 10);
```

Key points:
- Provides exclusive access to protected data
- Blocks threads that try to lock while already locked
- Guard automatically unlocks when dropped
- Lock poisoning occurs if thread panics while holding lock

### `RwLock<T>` - Reader-Writer Lock

```rust
use std::sync::RwLock;

let data = RwLock::new(vec![1, 2, 3]);

// Multiple readers
thread::scope(|s| {
    for _ in 0..5 {
        s.spawn(|| {
            let read = data.read().unwrap();
            println!("{:?}", *read);
        });
    }
});

// Single writer
{
    let mut write = data.write().unwrap();
    write.push(4);
}
```

Three states:
1. Unlocked
2. Locked by multiple readers (shared access)
3. Locked by one writer (exclusive access)

### `Cell<T>` and `RefCell<T>` - Single-Threaded Only

```rust
use std::cell::Cell;

// Cell: Copy values in/out, no borrows
let cell = Cell::new(5);
cell.set(10);
let value = cell.get();

// RefCell: Runtime borrow checking
use std::cell::RefCell;
let refcell = RefCell::new(vec![1, 2]);
refcell.borrow_mut().push(3);

// Neither is Send or Sync!
```

## Atomics - Lock-Free Synchronization

### Basic Atomic Types

```rust
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};

let flag = AtomicBool::new(false);
flag.store(true, Ordering::Relaxed);
let value = flag.load(Ordering::Relaxed);

let counter = AtomicUsize::new(0);
counter.fetch_add(1, Ordering::SeqCst);
```

### Memory Ordering

Five ordering levels (increasing strength):

1. **Relaxed** - No synchronization, only atomicity
2. **Release** - Store operation, synchronizes with Acquire
3. **Acquire** - Load operation, synchronizes with Release
4. **AcqRel** - Both Acquire and Release
5. **SeqCst** - Sequential consistency (strongest, slowest)

```rust
use std::sync::atomic::{AtomicBool, Ordering};

// Relaxed: Just atomicity
let ready = AtomicBool::new(false);
ready.store(true, Ordering::Relaxed);

// Release-Acquire: Synchronization
let data = AtomicI32::new(0);
thread::scope(|s| {
    s.spawn(|| {
        data.store(42, Ordering::Release);  // Writer
    });

    s.spawn(|| {
        while data.load(Ordering::Acquire) != 42 {}  // Reader
    });
});
```

### Compare-and-Swap

```rust
let value = AtomicUsize::new(5);

// Try to change 5 to 10
match value.compare_exchange(
    5,                      // Expected current value
    10,                     // New value
    Ordering::SeqCst,       // Success ordering
    Ordering::SeqCst        // Failure ordering
) {
    Ok(v) => println!("Changed from {}", v),
    Err(v) => println!("Already {}, not 5", v),
}
```

## Synchronization Patterns

### Thread Parking

```rust
use std::thread;
use std::time::Duration;

let handle = thread::spawn(|| {
    thread::park();  // Sleep until unparked
    println!("Unparked!");
});

thread::sleep(Duration::from_secs(1));
handle.thread().unpark();  // Wake up thread
handle.join().unwrap();
```

### Condition Variables

```rust
use std::sync::{Mutex, Condvar};
use std::collections::VecDeque;

let queue = Mutex::new(VecDeque::new());
let condvar = Condvar::new();

thread::scope(|s| {
    // Consumer
    s.spawn(|| {
        let mut q = queue.lock().unwrap();
        while q.is_empty() {
            q = condvar.wait(q).unwrap();  // Atomically unlock and wait
        }
        let item = q.pop_front();
        drop(q);  // Unlock before processing
        println!("Got: {:?}", item);
    });

    // Producer
    s.spawn(|| {
        queue.lock().unwrap().push_back(42);
        condvar.notify_one();  // Wake one waiter
    });
});
```

## Common Patterns

### Shared State with Arc + Mutex

```rust
use std::sync::{Arc, Mutex};

let counter = Arc::new(Mutex::new(0));

let handles: Vec<_> = (0..10).map(|_| {
    let counter = Arc::clone(&counter);
    thread::spawn(move || {
        let mut num = counter.lock().unwrap();
        *num += 1;
    })
}).collect();

for handle in handles {
    handle.join().unwrap();
}
```

### Message Passing with Channels

```rust
use std::sync::mpsc;

let (tx, rx) = mpsc::channel();

thread::spawn(move || {
    tx.send(42).unwrap();
});

let received = rx.recv().unwrap();
assert_eq!(received, 42);
```

### Once Initialization

```rust
use std::sync::Once;

static INIT: Once = Once::new();

INIT.call_once(|| {
    // Expensive initialization, guaranteed to run once
    println!("Initializing...");
});
```

## Data Races and Undefined Behavior

### What Prevents Data Races

Rust prevents data races through:
1. Borrowing rules (one writer XOR multiple readers)
2. Send/Sync traits (compiler-checked thread safety)
3. Type system (can't send non-Send types across threads)

### Safe Concurrent Patterns

```rust
// Pattern 1: Immutable shared data
let data = Arc::new(vec![1, 2, 3]);
// Multiple threads can read, none can write

// Pattern 2: Mutex for mutable access
let data = Arc::new(Mutex::new(vec![1, 2, 3]));
// One thread at a time can access

// Pattern 3: Atomic for single values
let counter = AtomicUsize::new(0);
// Lock-free atomic operations
```

### Unsafe and Send/Sync

When implementing unsafe code:

```rust
// Must manually verify thread safety
struct MyType {
    ptr: *mut i32,
}

// Only implement if actually thread-safe!
unsafe impl Send for MyType {}
unsafe impl Sync for MyType {}
```

## Performance Considerations

### Lock Contention

```rust
// Bad: Hold lock during slow operation
{
    let mut data = mutex.lock().unwrap();
    expensive_computation(&data);
}

// Good: Release lock early
let snapshot = {
    let data = mutex.lock().unwrap();
    data.clone()
};
expensive_computation(&snapshot);
```

### Choosing Synchronization

- **Atomics**: Fastest, but limited to simple types
- **Mutex**: General purpose, moderate overhead
- **RwLock**: When reads greatly outnumber writes
- **Channels**: Message passing, moves data between threads
