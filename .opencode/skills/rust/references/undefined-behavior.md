# Undefined Behavior

Critical reference on what constitutes undefined behavior in Rust and how to avoid it.

## What is Undefined Behavior?

**Undefined behavior (UB)** is code that violates Rust's safety rules. When UB occurs:
- The compiler may assume it never happens
- Optimizations based on this assumption can break your program
- Effects can propagate both forward and backward in execution
- The entire program becomes unreliable, not just the unsafe block

**Key principle**: In safe Rust, UB is impossible. In unsafe Rust, it's your responsibility to prevent it.

## List of Undefined Behaviors

### Data Races

**Never allowed**: Two or more threads accessing the same memory location where at least one is a write, without synchronization.

```rust
// UB: Data race
static mut COUNTER: i32 = 0;

thread::spawn(|| unsafe {
    COUNTER += 1;  // UB: Race with other thread
});
unsafe {
    COUNTER += 1;  // UB: Race with spawned thread
}

// Fix: Use atomics or mutexes
static COUNTER: AtomicI32 = AtomicI32::new(0);
COUNTER.fetch_add(1, Ordering::SeqCst);
```

### Dangling Pointers

**Never allowed**: Accessing memory through a pointer when the data no longer exists.

```rust
// UB: Dangling reference
let r: &i32;
{
    let x = 42;
    r = &x;
}  // x dropped here
// println!("{}", r);  // UB: r is dangling

// UB: Use after free
let ptr = Box::into_raw(Box::new(42));
unsafe {
    drop(Box::from_raw(ptr));
    let value = *ptr;  // UB: ptr is dangling
}

// Zero-sized types are never dangling
let ptr: *const () = std::ptr::null();
unsafe {
    let _unit = *ptr;  // OK: zero-sized, never dangling
}
```

### Misaligned Pointers

**Never allowed**: Loading from or storing to a misaligned pointer.

```rust
// UB: Misaligned access
#[repr(packed)]
struct Packed {
    a: u8,
    b: u32,  // May not be 4-byte aligned
}

let p = Packed { a: 1, b: 2 };
// let r = &p.b;  // Error: can't take reference to packed field
unsafe {
    let ptr = std::ptr::addr_of!(p.b);
    let value = *ptr;  // UB if ptr is misaligned
}

// Fix: Use read_unaligned
unsafe {
    let ptr = std::ptr::addr_of!(p.b);
    let value = ptr.read_unaligned();  // OK
}
```

### Violating Pointer Aliasing Rules

**The Rules:**
- `&T`: Shared reference, can have multiple, data cannot be mutated (except through `UnsafeCell`)
- `&mut T`: Exclusive reference, must be the only reference, can mutate
- `Box<T>`: Like `&mut T`, exclusive ownership

```rust
// UB: Mutable alias
let mut x = 42;
let r1 = &mut x;
let r2 = &mut x;  // Compiler prevents this in safe code
// *r1 += 1;  // Would be UB if r2 existed

// UB: Reading through one pointer while writing through another
unsafe {
    let mut x = 42;
    let ptr1 = &x as *const i32;
    let ptr2 = &mut x as *mut i32;
    *ptr2 = 10;  // Invalidates ptr1
    let val = *ptr1;  // UB: ptr1 was invalidated
}

// OK: No overlap in reference lifetimes
let mut x = 42;
{
    let r = &x;
    println!("{}", r);
}  // r ends
let r_mut = &mut x;
*r_mut = 10;
```

### Mutating Immutable Data

**Never allowed**: Mutating through a shared reference (except via `UnsafeCell`).

```rust
// UB: Mutating immutable data
let x = 42;
let ptr = &x as *const i32 as *mut i32;
unsafe {
    *ptr = 10;  // UB: x is immutable
}

// UB: Mutating static without UnsafeCell
static VALUE: i32 = 42;
let ptr = &VALUE as *const i32 as *mut i32;
unsafe {
    *ptr = 10;  // UB: static is immutable
}

// OK: Using UnsafeCell
use std::cell::UnsafeCell;
static VALUE: UnsafeCell<i32> = UnsafeCell::new(42);
unsafe {
    *VALUE.get() = 10;  // OK: UnsafeCell allows interior mutability
}
```

### Invalid Values

Certain types have restricted valid values. Producing an invalid value is UB.

#### Invalid Primitive Values

```rust
// UB: Invalid bool
let b: bool = unsafe { std::mem::transmute(2u8) };  // UB: bool must be 0 or 1

// UB: Invalid char
let c: char = unsafe { std::mem::transmute(0xD800u32) };  // UB: surrogate code point

// UB: Null function pointer
let f: fn() = unsafe { std::mem::transmute(0usize) };  // UB: fn pointer must be non-null

// OK: Uninitialized integers/floats/raw pointers
let x: i32 = unsafe { std::mem::MaybeUninit::uninit().assume_init() };  // OK
```

#### Invalid References

```rust
// UB: Null reference
let r: &i32 = unsafe { std::mem::transmute(0usize) };  // UB

// UB: Dangling reference
let r: &i32 = unsafe {
    std::mem::transmute(0xdeadbeefusize)  // UB: doesn't point to valid memory
};

// UB: Misaligned reference
let bytes = [0u8; 8];
let r: &u64 = unsafe {
    &*(bytes.as_ptr().add(1) as *const u64)  // UB: misaligned
};

// UB: Reference to invalid value
let r: &bool = unsafe {
    &*(2u8 as *const u8 as *const bool)  // UB: bool must be 0 or 1
};
```

#### Invalid Enums

```rust
enum MyEnum {
    A,
    B,
    C,
}

// UB: Invalid discriminant
let e: MyEnum = unsafe { std::mem::transmute(3u8) };  // UB: only 0, 1, 2 valid
```

#### Invalid Strings

```rust
// UB: Invalid UTF-8 in str
let s: &str = unsafe {
    std::str::from_utf8_unchecked(&[0xFF, 0xFF])  // UB: invalid UTF-8
};

// OK: Invalid UTF-8 in [u8]
let bytes: &[u8] = &[0xFF, 0xFF];  // OK
```

### Calling Functions with Wrong ABI

```rust
// UB: Wrong calling convention
extern "C" fn c_func() {}

let f: extern "stdcall" fn() = unsafe {
    std::mem::transmute(c_func as *const ())  // UB: ABI mismatch
};
unsafe { f(); }  // UB when called
```

### Unwinding Across FFI Boundaries

```rust
// UB: Unwinding into C code
extern "C" fn callback() {
    panic!("oops");  // UB: unwinding across C boundary
}

// Fix: Use "C-unwind" or catch the panic
extern "C-unwind" fn callback() {
    panic!("oops");  // OK: can unwind
}

// Or catch panics
extern "C" fn callback() {
    let result = std::panic::catch_unwind(|| {
        might_panic();
    });
    // Handle error without unwinding
}
```

### Invoking Undefined Compiler Intrinsics

```rust
// Many intrinsics have safety requirements
use std::intrinsics;

unsafe {
    // UB: Index out of bounds
    let arr = [1, 2, 3];
    intrinsics::unchecked_add(arr.len(), 10);  // May be UB if overflow
}
```

### Using Unsupported Platform Features

```rust
// UB: Using features not available on current CPU
#[target_feature(enable = "avx2")]
unsafe fn use_avx2() {
    // If CPU doesn't support AVX2, this is UB
}

// Check at runtime
if is_x86_feature_detected!("avx2") {
    unsafe { use_avx2(); }  // OK
}
```

## Special Cases

### Uninitialized Memory

```rust
// UB: Reading uninitialized memory (except in some cases)
let x: i32;
// println!("{}", x);  // Compiler prevents this

unsafe {
    let x: i32 = std::mem::MaybeUninit::uninit().assume_init();
    let y = x;  // UB: read uninitialized i32
}

// OK: Uninitialized in unions and padding
union U {
    x: u8,
    y: u16,
}
let u = U { x: 1 };
unsafe {
    let y = u.y;  // OK: reading uninitialized padding bytes
}
```

### Inline Assembly

```rust
use std::arch::asm;

unsafe {
    // Must follow inline assembly rules
    asm!(
        "add {}, 1",
        inout(reg) x,
        options(nomem, nostack)  // Must declare what you touch
    );
}
```

### Const Evaluation

```rust
// UB: Pointer-to-integer transmutation in const context
const PTR_AS_INT: usize = unsafe {
    std::mem::transmute::<*const i32, usize>(std::ptr::null())  // UB
};

// OK in runtime context
let ptr_as_int: usize = unsafe {
    std::mem::transmute(std::ptr::null::<i32>())  // OK at runtime
};
```

## How to Avoid UB

### Use Safe Abstractions

```rust
// Instead of raw pointers, use safe types
let data = vec![1, 2, 3];  // Not: *mut i32

// Instead of transmute, use safe conversions
let bytes = x.to_ne_bytes();  // Not: transmute(x)
```

### Document Safety Requirements

```rust
/// # Safety
///
/// - `ptr` must be valid for reads
/// - `ptr` must be properly aligned
/// - `ptr` must point to an initialized value
pub unsafe fn read_value(ptr: *const i32) -> i32 {
    *ptr
}
```

### Use Validation

```rust
// Validate before using unsafe code
pub unsafe fn slice_from_raw_parts(ptr: *const u8, len: usize) -> &[u8] {
    // In reality, you'd want more validation
    assert!(!ptr.is_null());
    std::slice::from_raw_parts(ptr, len)
}
```

### Use Miri

Miri is an interpreter that detects UB:

```bash
cargo +nightly miri test
```

Miri can catch:
- Use of uninitialized memory
- Use after free
- Double free
- Invalid pointer arithmetic
- Data races
- And more

### Use Tools

- **Address Sanitizer**: Detects memory errors
- **Thread Sanitizer**: Detects data races
- **Undefined Behavior Sanitizer**: Detects various UB

```bash
RUSTFLAGS="-Z sanitizer=address" cargo run
RUSTFLAGS="-Z sanitizer=thread" cargo run
```

## Key Takeaways

1. **Safe Rust prevents UB completely** - If you write only safe Rust, you cannot cause UB
2. **Unsafe is a promise** - When you write `unsafe`, you promise to uphold invariants
3. **UB can time travel** - Effects can appear before the UB code runs
4. **Document safety requirements** - Always document what unsafe code requires
5. **Use tools** - Miri, sanitizers, and fuzzing can catch UB
6. **Encapsulate unsafe** - Make safe public APIs around unsafe code
