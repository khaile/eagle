# Rust Style Guide

Official formatting and naming conventions for Rust code.

## Formatting Basics

### Indentation and Line Width

- **Use spaces, not tabs**
- **4 spaces per indentation level**
- **Maximum line width: 100 characters**

```rust
fn example() {
    if condition {
        do_something();
    }
}
```

### Block vs Visual Indent

**Prefer block indent:**

```rust
// Good: Block indent
a_function_call(
    argument1,
    argument2,
    argument3,
);

// Bad: Visual indent (causes larger diffs)
a_function_call(argument1,
                argument2,
                argument3);
```

### Trailing Commas

Use trailing commas when the closing delimiter is on a new line:

```rust
// Good
let array = [
    element1,
    element2,
    element3,
];

function_call(
    arg1,
    arg2,
);

// Bad
let array = [
    element1,
    element2,
    element3
];
```

Benefits:
- Easier to reorder items
- Cleaner diffs when adding/removing items
- Easier copy-paste

### Blank Lines

Separate items with 0 or 1 blank line:

```rust
fn foo() {
    let x = 1;

    let y = 2;  // Optional blank line for readability
    let z = 3;
}

fn bar() {}

fn baz() {}
```

## Naming Conventions

### Casing

| Item | Convention |
|------|------------|
| Modules | `snake_case` |
| Types | `UpperCamelCase` |
| Traits | `UpperCamelCase` |
| Enum variants | `UpperCamelCase` |
| Functions | `snake_case` |
| Methods | `snake_case` |
| Statics | `SCREAMING_SNAKE_CASE` |
| Constants | `SCREAMING_SNAKE_CASE` |
| Type parameters | `UpperCamelCase` |
| Lifetimes | `'lowercase` |

```rust
// Module
mod my_module;

// Type
struct MyStruct;
enum MyEnum { VariantA, VariantB }

// Trait
trait MyTrait {}

// Function
fn my_function() {}

// Method
impl MyStruct {
    fn my_method(&self) {}
}

// Static/Constant
static MAX_SIZE: usize = 100;
const PI: f64 = 3.14159;

// Generic type parameter
struct Container<T> { item: T }

// Lifetime
fn longest<'a>(x: &'a str, y: &'a str) -> &'a str { x }
```

### Acronyms in Names

Treat acronyms as words in `UpperCamelCase`:

```rust
// Good
struct HttpResponse;
struct JsonParser;

// Bad
struct HTTPResponse;
struct JSONParser;
```

But keep them uppercase in `SCREAMING_SNAKE_CASE`:

```rust
const MAX_HTTP_CONNECTIONS: usize = 100;
```

## Items

### Modules

```rust
// Module declaration
mod my_module;
mod nested {
    pub mod inner;
}

// Module attributes on separate lines
#[cfg(test)]
#[allow(unused)]
mod tests {
    use super::*;
}
```

### Imports (Use Statements)

```rust
// Group imports: std, external crates, local
use std::collections::HashMap;
use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::models::User;
use crate::utils;

// Prefer explicit imports over glob
use std::collections::HashMap;  // Good
// use std::collections::*;     // Bad

// Exception: preludes
use std::io::prelude::*;
```

### Functions

```rust
// Function signature formatting
fn short_function(x: i32, y: i32) -> i32 {
    x + y
}

// Long signature: break after opening paren
fn long_function_name(
    parameter1: VeryLongTypeName,
    parameter2: AnotherLongTypeName,
) -> ReturnType {
    // body
}

// Where clauses
fn generic_function<T, U>(x: T, y: U) -> T
where
    T: Clone + Debug,
    U: Display,
{
    // body
}
```

### Structs

```rust
// Small struct: single line
struct Point { x: i32, y: i32 }

// Normal struct: field per line
struct Rectangle {
    width: u32,
    height: u32,
}

// Struct with where clause
struct Container<T>
where
    T: Clone + Debug,
{
    items: Vec<T>,
}

// Field attributes
struct Config {
    #[serde(rename = "apiKey")]
    api_key: String,
    timeout: Duration,
}
```

### Enums

```rust
// Simple enum
enum Color { Red, Green, Blue }

// Enum with data
enum Message {
    Quit,
    Move { x: i32, y: i32 },
    Write(String),
    ChangeColor(i32, i32, i32),
}

// Long variant: break like struct
enum LongEnum {
    VariantWithManyFields {
        field1: VeryLongTypeName,
        field2: AnotherLongType,
        field3: YetAnotherType,
    },
}
```

### Trait Definitions

```rust
// Simple trait
trait Drawable {
    fn draw(&self);
}

// Trait with associated types
trait Container {
    type Item;
    fn add(&mut self, item: Self::Item);
}

// Trait with where clause
trait Process<T>
where
    T: Clone,
{
    fn process(&self, item: T) -> T;
}
```

### Implementations

```rust
// Simple impl
impl MyStruct {
    fn new() -> Self {
        MyStruct
    }
}

// Generic impl with where clause
impl<T> Container<T>
where
    T: Clone + Debug,
{
    fn new() -> Self {
        Container { items: Vec::new() }
    }
}

// Trait impl
impl Display for MyStruct {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "MyStruct")
    }
}
```

## Expressions

### Match

```rust
// Match expressions
match value {
    Pattern1 => expression1,
    Pattern2 => {
        statement1;
        statement2;
        expression2
    }
    Pattern3 | Pattern4 => expression3,
    _ => default_expression,
}

// Align fat arrows if it improves readability
match value {
    ShortPattern      => short_expr,
    LongerPattern     => longer_expr,
    VeryLongPattern   => very_long_expr,
}
```

### If/Else

```rust
// Standard if
if condition {
    do_something();
}

// If-else
if condition {
    do_something();
} else {
    do_something_else();
}

// If-else-if
if condition1 {
    branch1();
} else if condition2 {
    branch2();
} else {
    branch3();
}

// Single-expression bodies can be on one line
if condition { return; }
```

### Loops

```rust
// For loops
for item in collection {
    process(item);
}

for (i, item) in collection.iter().enumerate() {
    process(i, item);
}

// While
while condition {
    do_something();
}

// Loop
loop {
    if exit_condition {
        break;
    }
}
```

### Closures

```rust
// Short closure
let add = |x, y| x + y;

// Multi-line closure
let process = |item| {
    let processed = transform(item);
    validate(processed)
};

// Closure with explicit types
let multiply = |x: i32, y: i32| -> i32 {
    x * y
};
```

### Method Chains

```rust
// Short chains: single line
let result = data.iter().map(|x| x * 2).collect();

// Long chains: one method per line
let result = data
    .iter()
    .filter(|x| x.is_positive())
    .map(|x| x * 2)
    .collect::<Vec<_>>();

// Very long method arguments
let result = data
    .iter()
    .map(|x| {
        some_complex_transformation(x)
    })
    .collect();
```

## Types

### Type Aliases

```rust
type Result<T> = std::result::Result<T, Error>;
type Callback = Box<dyn Fn(i32) -> i32>;

// With generics
type HashMap<K, V> = std::collections::HashMap<K, V, DefaultHasher>;
```

### References and Pointers

```rust
// Space after &, *, mut
let r: &i32 = &x;
let r: &mut i32 = &mut x;
let p: *const i32 = &x as *const i32;
let p: *mut i32 = &mut x as *mut i32;
```

## Comments

### Line Comments

```rust
// Single space after //
// This is a comment

// Not:
//This is a comment
```

### Block Comments

```rust
/* Block comment with spaces around */

/*
 * Multi-line block comment
 * with consistent formatting
 */
```

### Doc Comments

```rust
/// Documentation for the following item.
/// Use /// for outer doc comments.
pub fn documented_function() {}

//! Documentation for the enclosing item.
//! Use //! for inner doc comments.

/// Function with detailed documentation.
///
/// # Examples
///
/// ```
/// use mylib::add;
/// assert_eq!(add(2, 2), 4);
/// ```
///
/// # Errors
///
/// Returns an error if...
///
/// # Panics
///
/// Panics if...
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

### Comment Placement

```rust
// Good: Comment on its own line
// This explains the next statement
let x = 42;

// Good: Comment after code (single space before)
let y = 10;  // Short explanation

// Bad: No space before comment
let z = 5;// Bad
```

## Attributes

```rust
// Attributes on separate lines
#[derive(Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct User {
    #[serde(rename = "firstName")]
    first_name: String,
    last_name: String,
}

// Inner attributes
#![allow(unused)]
#![cfg_attr(test, deny(warnings))]

// Single derive attribute only
#[derive(Debug, Clone, PartialEq, Eq)]  // Good
// Not multiple derives:
// #[derive(Debug)]
// #[derive(Clone)]
```

## Cargo.toml Conventions

```toml
[package]
name = "my-crate"
version = "0.1.0"
authors = ["Your Name <you@example.com>"]
edition = "2021"
rust-version = "1.70"
description = "A short description"
documentation = "https://docs.rs/my-crate"
readme = "README.md"
homepage = "https://github.com/user/repo"
repository = "https://github.com/user/repo"
license = "MIT OR Apache-2.0"
keywords = ["keyword1", "keyword2"]
categories = ["category"]

[dependencies]
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.0", features = ["full"] }

[dev-dependencies]
proptest = "1.0"
```

## Tools

### rustfmt

Apply automatic formatting:

```bash
cargo fmt
```

### clippy

Lint your code:

```bash
cargo clippy
```

### IDE Integration

Most IDEs support:
- Format on save
- Automatic import organization
- Real-time style warnings
