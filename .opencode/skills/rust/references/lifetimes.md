# Lifetimes

## When Lifetimes Are Needed

Lifetime annotations are required when:
1. The compiler cannot determine which reference a function returns
2. A struct/enum contains references
3. Multiple references with different lifetimes interact

## Why Lifetimes Exist

The compiler needs to prevent dangling references. Consider:

```rust
fn max_of_refs(a: &i32, b: &i32) -> &i32 {
    if *a > *b { a } else { b }
}

fn complex_function(a: &i32) -> &i32 {
    let b = 2;
    max_of_refs(a, &b)  // Problem: might return reference to b
}  // b goes out of scope here - dangling reference!
```

Without lifetime annotations, the compiler can't verify this is safe.

## Lifetime Annotation Syntax

Lifetimes are named with an apostrophe: `'a`, `'b`, `'static`

### Function Signatures

```rust
// Explicit lifetime: return value lives as long as the input
fn longest<'a>(x: &'a str, y: &'a str) -> &'a str {
    if x.len() > y.len() { x } else { y }
}

// Multiple lifetimes: return tied to first parameter only
fn first_word<'a, 'b>(x: &'a str, y: &'b str) -> &'a str {
    x.split_whitespace().next().unwrap()
}
```

### Reading Lifetime Annotations

`fn foo<'a>(x: &'a i32) -> &'a i32`
- "For some lifetime 'a"
- "Take a reference to an i32 that lives for 'a"
- "Return a reference to an i32 that lives for 'a"
- "The returned reference lives as long as the input"

## Lifetime Elision Rules

The compiler automatically infers lifetimes in simple cases:

1. Each input reference gets its own lifetime
2. If there's exactly one input lifetime, it's assigned to all outputs
3. If there's a `&self` or `&mut self`, its lifetime is assigned to all outputs

Examples where elision works:

```rust
// Elided (compiler infers)
fn first_word(s: &str) -> &str

// Explicit (what compiler sees)
fn first_word<'a>(s: &'a str) -> &'a str

// Elided for methods
impl Foo {
    fn get_data(&self) -> &Data
}

// Explicit
impl Foo {
    fn get_data<'a>(&'a self) -> &'a Data
}
```

## Lifetimes in Structs

Structs holding references need lifetime annotations:

```rust
// Struct lifetime
struct Excerpt<'a> {
    part: &'a str,
}

impl<'a> Excerpt<'a> {
    fn announce(&self) -> &str {
        println!("Excerpt: {}", self.part);
        self.part
    }
}

// Usage
fn main() {
    let novel = String::from("Call me Ishmael. Some years ago...");
    let first_sentence = novel.split('.').next().unwrap();
    let excerpt = Excerpt { part: first_sentence };
}  // excerpt must be dropped before novel
```

### Multiple Lifetimes in Structs

```rust
struct Context<'a, 'b> {
    config: &'a Config,
    data: &'b Data,
}
```

## Lifetime Bounds

### Trait Bounds with Lifetimes

```rust
// T must live at least as long as 'a
fn print_ref<'a, T>(x: &'a T)
where
    T: Debug + 'a
{
    println!("{:?}", x);
}
```

### Lifetime Bounds on Types

```rust
struct Ref<'a, T: 'a> {
    value: &'a T,
}

// Means: T must outlive lifetime 'a
```

## Special Lifetimes

### `'static` Lifetime

Two meanings:
1. **Static variable**: Lives for entire program duration
2. **Static bound**: Value can live for entire program duration (doesn't have to)

```rust
// String literal has 'static lifetime
let s: &'static str = "hello";

// Static variable
static HELLO: &str = "hello";

// Function requiring 'static (thread spawn)
use std::thread;
thread::spawn(|| {
    println!("hello");
});
```

### Anonymous Lifetime `'_`

Explicit placeholder when lifetime is obvious:

```rust
// Explicit lifetime
fn print<'a>(x: &'a i32) { }

// Anonymous lifetime
fn print(x: &'_ i32) { }
```

## Lifetime Coercion

Shorter lifetimes can be coerced to longer ones when safe:

```rust
fn choose<'a, 'b>(first: &'a str, _second: &'b str) -> &'a str {
    first
}

fn main() {
    let outer = String::from("outer");
    {
        let inner = String::from("inner");
        let result = choose(&outer, &inner);
        // OK: 'outer outlives this scope
    }
}
```

## Common Lifetime Patterns

### Returning References from Functions

```rust
// OK: Returns reference to input
fn first_element<'a>(list: &'a [i32]) -> &'a i32 {
    &list[0]
}

// Error: Can't return reference to local
fn dangling() -> &String {
    let s = String::from("hello");
    &s
}  // s is dropped, reference would dangle

// Fix: Return owned data
fn not_dangling() -> String {
    String::from("hello")
}
```

### Struct Methods with Lifetimes

```rust
struct Parser<'a> {
    input: &'a str,
}

impl<'a> Parser<'a> {
    // Return lifetime tied to self
    fn next_token(&mut self) -> Option<&'a str> {
        // Parse and return slice of self.input
    }
}
```

### Higher-Ranked Trait Bounds (HRTB)

For functions that work with any lifetime:

```rust
fn call_with_ref<F>(f: F)
where
    F: for<'a> Fn(&'a i32)
{
    let value = 42;
    f(&value);
}
```

## Debugging Lifetime Errors

Common errors and solutions:

### Error: Lifetime may not live long enough

```rust
// Problem
fn combine<'a, 'b>(x: &'a str, y: &'b str) -> &'a str {
    format!("{}{}", x, y).as_str()  // Error: returns temporary
}

// Fix: Return owned String
fn combine(x: &str, y: &str) -> String {
    format!("{}{}", x, y)
}
```

### Error: Borrowed value does not live long enough

```rust
// Problem
let result;
{
    let x = String::from("hello");
    result = &x;
}  // x dropped here
// println!("{}", result);  // Error: x doesn't live long enough

// Fix: Extend lifetime
let x = String::from("hello");
let result = &x;
println!("{}", result);
```
