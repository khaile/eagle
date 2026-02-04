# Ownership and Borrowing

## Core Principles

### Ownership Rules
1. **Resources can only have one owner** - This prevents resources from being freed more than once
2. **Assignment and function calls transfer ownership** - Known as a "move" in Rust
3. **After moving, previous owner can no longer be used** - Prevents dangling pointers

### Move Semantics

When doing assignments (`let x = y`) or passing function arguments by value (`foo(x)`), ownership transfers:

```rust
// Heap allocated data moves ownership
let a = Box::new(5i32);
let b = a;  // a is no longer valid

// Stack allocated data with Copy trait is copied, not moved
let x = 5u32;
let y = x;  // Both x and y are valid
```

### Partial Moves

Destructuring can cause partial moves:

```rust
struct Person {
    name: String,
    age: u32,
}

let person = Person {
    name: String::from("Alice"),
    age: 30,
};

let Person { name, ref age } = person;
// name is moved, age is borrowed
// person.name is no longer valid
// person.age is still valid
```

## Borrowing

### Immutable References (`&T`)

- Multiple immutable references are allowed simultaneously
- Data cannot be modified through an immutable reference
- The borrowed data must outlive the reference

```rust
fn borrow_book(book: &Book) {
    // Can read but not modify
}

let book = Book { title: "Rust", year: 2015 };
borrow_book(&book);
borrow_book(&book);  // OK: multiple immutable borrows
```

### Mutable References (`&mut T`)

- Only ONE mutable reference is allowed at a time
- No immutable references can exist while a mutable reference exists
- Guarantees exclusive access for safe mutation

```rust
fn new_edition(book: &mut Book) {
    book.year = 2024;  // Can modify
}

let mut book = Book { title: "Rust", year: 2015 };
new_edition(&mut book);  // OK

// Error: cannot have two mutable references
// let r1 = &mut book;
// let r2 = &mut book;
```

### Borrowing Rules

The fundamental rule that prevents data races:
- You can have EITHER:
  - Multiple immutable references (`&T`)
  - OR exactly one mutable reference (`&mut T`)
- But NOT both at the same time

### Aliasing and Mutability

You cannot have aliasing AND mutability at the same time:

```rust
let mut data = vec![1, 2, 3];
let ptr1 = &mut data;
// let ptr2 = &data;  // Error: can't borrow as immutable while mutable borrow exists
ptr1.push(4);
```

### Reference Patterns

Using `ref` in patterns creates a reference instead of moving:

```rust
let c = 'Q';

let ref ref_c1 = c;  // ref_c1 is &char
let ref_c2 = &c;     // ref_c2 is &char

// In pattern matching
match c {
    ref r => println!("ref: {}", r),  // r is &char
}
```

## Common Patterns

### Borrowing vs Moving in Functions

```rust
// Takes ownership, value is moved
fn consume(s: String) {
    println!("{}", s);
}  // s is dropped here

// Borrows immutably, value can be used after
fn borrow(s: &String) {
    println!("{}", s);
}  // s reference goes out of scope, original still valid

// Borrows mutably, can modify but must return access
fn modify(s: &mut String) {
    s.push_str(" world");
}  // mutable borrow ends, can use original again
```

### Return References

Functions can return references to borrowed data:

```rust
fn first_word(s: &str) -> &str {
    &s[..1]
}

// The returned reference is tied to the input lifetime
let text = String::from("hello");
let first = first_word(&text);
```
