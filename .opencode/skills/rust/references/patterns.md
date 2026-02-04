# Rust Design Patterns and Idioms

Common patterns, idioms, and anti-patterns in Rust programming.

## Idioms

### Use Borrowed Types for Arguments

```rust
// Bad: Takes ownership unnecessarily
fn process_string(s: String) { ... }

// Good: Borrows, works with String, &str, etc.
fn process_string(s: &str) { ... }

// Bad: Too specific
fn print_vec(v: Vec<i32>) { ... }

// Good: Generic over anything that can be a slice
fn print_vec(v: &[i32]) { ... }
```

### Concatenating Strings with `format!`

```rust
// Bad: Verbose and error-prone
let mut result = String::new();
result.push_str("Hello");
result.push_str(", ");
result.push_str(&name);
result.push_str("!");

// Good: Clear and concise
let result = format!("Hello, {}!", name);
```

### Constructor Pattern

```rust
pub struct Config {
    pub debug: bool,
    timeout: Duration,
}

impl Config {
    pub fn new() -> Self {
        Self {
            debug: false,
            timeout: Duration::from_secs(30),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}
```

### Collections Are Smart Pointers

```rust
// These are equivalent for iteration
let v = vec![1, 2, 3];

for i in &v { ... }
for i in v.iter() { ... }

for i in &mut v { ... }
for i in v.iter_mut() { ... }

for i in v { ... }
for i in v.into_iter() { ... }
```

### Temporary Mutability

```rust
// Good: Mutation during initialization, immutable after
let data = {
    let mut data = Vec::new();
    data.push(1);
    data.push(2);
    data.push(3);
    data  // Immutable from here
};

// Or with a function
let data = {
    let mut data = Vec::new();
    populate(&mut data);
    data
};
```

## Design Patterns

### Newtype Pattern

Provide type safety with zero runtime cost:

```rust
pub struct Seconds(u64);
pub struct Meters(f64);

impl Seconds {
    pub fn new(value: u64) -> Self {
        Seconds(value)
    }

    pub fn as_secs(&self) -> u64 {
        self.0
    }
}

// Can implement traits
impl std::ops::Add for Seconds {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Seconds(self.0 + other.0)
    }
}
```

### Builder Pattern

```rust
pub struct Server {
    host: String,
    port: u16,
    timeout: Duration,
    workers: usize,
}

pub struct ServerBuilder {
    host: String,
    port: u16,
    timeout: Duration,
    workers: usize,
}

impl ServerBuilder {
    pub fn new(host: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port: 8080,
            timeout: Duration::from_secs(30),
            workers: 4,
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub fn build(self) -> Server {
        Server {
            host: self.host,
            port: self.port,
            timeout: self.timeout,
            workers: self.workers,
        }
    }
}

// Usage
let server = ServerBuilder::new("localhost")
    .port(3000)
    .workers(8)
    .build();
```

### RAII Guards

```rust
pub struct MutexGuard<'a, T> {
    lock: &'a Mutex<T>,
    // ... other fields
}

impl<'a, T> Drop for MutexGuard<'a, T> {
    fn drop(&mut self) {
        // Automatically unlock when guard goes out of scope
        self.lock.unlock();
    }
}

impl<'a, T> Deref for MutexGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        // Provide access to protected data
    }
}
```

### Visitor Pattern

```rust
trait Visitor {
    fn visit_a(&mut self, a: &A);
    fn visit_b(&mut self, b: &B);
}

trait Visitable {
    fn accept<V: Visitor>(&self, visitor: &mut V);
}

struct A { ... }
struct B { ... }

impl Visitable for A {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_a(self);
    }
}

impl Visitable for B {
    fn accept<V: Visitor>(&self, visitor: &mut V) {
        visitor.visit_b(self);
    }
}
```

### Strategy Pattern with Traits

```rust
trait CompressionStrategy {
    fn compress(&self, data: &[u8]) -> Vec<u8>;
}

struct GzipCompression;
impl CompressionStrategy for GzipCompression {
    fn compress(&self, data: &[u8]) -> Vec<u8> { ... }
}

struct ZstdCompression;
impl CompressionStrategy for ZstdCompression {
    fn compress(&self, data: &[u8]) -> Vec<u8> { ... }
}

struct Compressor<S: CompressionStrategy> {
    strategy: S,
}

impl<S: CompressionStrategy> Compressor<S> {
    fn compress(&self, data: &[u8]) -> Vec<u8> {
        self.strategy.compress(data)
    }
}
```

### Extension Trait Pattern

```rust
// Extend existing types with new functionality
pub trait VecExt<T> {
    fn partition_by<F>(&self, f: F) -> (Vec<T>, Vec<T>)
    where
        F: Fn(&T) -> bool,
        T: Clone;
}

impl<T> VecExt<T> for Vec<T> {
    fn partition_by<F>(&self, f: F) -> (Vec<T>, Vec<T>)
    where
        F: Fn(&T) -> bool,
        T: Clone,
    {
        let mut left = Vec::new();
        let mut right = Vec::new();
        for item in self {
            if f(item) {
                left.push(item.clone());
            } else {
                right.push(item.clone());
            }
        }
        (left, right)
    }
}
```

### Fold Pattern

```rust
// Use fold instead of explicit loops
let sum: i32 = vec![1, 2, 3, 4]
    .iter()
    .fold(0, |acc, x| acc + x);

// Complex example
#[derive(Default)]
struct Stats {
    count: usize,
    sum: i32,
    min: Option<i32>,
    max: Option<i32>,
}

let stats = numbers.iter().fold(Stats::default(), |mut stats, &n| {
    stats.count += 1;
    stats.sum += n;
    stats.min = Some(stats.min.map_or(n, |m| m.min(n)));
    stats.max = Some(stats.max.map_or(n, |m| m.max(n)));
    stats
});
```

### Option and Result Combinators

```rust
// Instead of explicit matching
let value = match some_option {
    Some(v) => v * 2,
    None => return,
};

// Use map
let value = some_option.map(|v| v * 2)?;

// Chain operations
let result = parse_input(&input)
    .and_then(|data| validate(data))
    .and_then(|data| process(data))
    .map(|data| format_output(data))
    .unwrap_or_else(|e| format!("Error: {}", e));
```

### Type State Pattern

```rust
// Encode state in types
struct Locked;
struct Unlocked;

struct Door<State> {
    _state: PhantomData<State>,
}

impl Door<Locked> {
    fn new() -> Self {
        Door { _state: PhantomData }
    }

    fn unlock(self) -> Door<Unlocked> {
        println!("Unlocking door");
        Door { _state: PhantomData }
    }
}

impl Door<Unlocked> {
    fn lock(self) -> Door<Locked> {
        println!("Locking door");
        Door { _state: PhantomData }
    }

    fn open(&self) {
        println!("Opening door");
    }
}

// Usage - compiler prevents opening locked door
let door = Door::<Locked>::new();
// door.open();  // Compile error!
let door = door.unlock();
door.open();  // OK
```

### Prefer Small Crates

```rust
// Instead of one large crate
my_project/
├── Cargo.toml
└── src/
    ├── lib.rs (10,000 lines)
    └── ...

// Prefer multiple focused crates
my_project/
├── Cargo.toml
├── core/
│   ├── Cargo.toml
│   └── src/lib.rs
├── parser/
│   ├── Cargo.toml
│   └── src/lib.rs
└── cli/
    ├── Cargo.toml
    └── src/main.rs
```

## Anti-Patterns

### Clone to Satisfy Borrow Checker

```rust
// Anti-pattern: Cloning to avoid borrow checker
let s = String::from("hello");
let s_clone = s.clone();  // Unnecessary clone
process(&s_clone);
println!("{}", s);

// Better: Just borrow
let s = String::from("hello");
process(&s);
println!("{}", s);
```

### Deref Polymorphism

```rust
// Anti-pattern: Using Deref for inheritance-like behavior
struct MyString {
    inner: String,
}

impl Deref for MyString {
    type Target = String;
    fn deref(&self) -> &String {
        &self.inner
    }
}

// Better: Explicit methods or AsRef
impl MyString {
    fn as_str(&self) -> &str {
        &self.inner
    }
}

impl AsRef<str> for MyString {
    fn as_ref(&self) -> &str {
        &self.inner
    }
}
```

### `#[deny(warnings)]`

```rust
// Anti-pattern: Deny all warnings in library code
#![deny(warnings)]  // Don't do this in libraries!

// Better: Deny specific warnings you care about
#![deny(missing_docs)]
#![deny(unsafe_code)]

// Or use it locally during development
#![cfg_attr(test, deny(warnings))]
```

### Over-using `unwrap()`

```rust
// Anti-pattern: Unwrap everywhere
fn process(input: &str) -> i32 {
    input.parse::<i32>().unwrap() * 2
}

// Better: Propagate errors
fn process(input: &str) -> Result<i32, ParseIntError> {
    Ok(input.parse::<i32>()? * 2)
}

// Or provide a default
fn process(input: &str) -> i32 {
    input.parse::<i32>().unwrap_or(0) * 2
}
```

### Large Monolithic Functions

```rust
// Anti-pattern: One giant function
fn process_data(data: &[u8]) -> Result<Output> {
    // 500 lines of code doing everything
}

// Better: Break into focused functions
fn process_data(data: &[u8]) -> Result<Output> {
    let parsed = parse(data)?;
    let validated = validate(parsed)?;
    let transformed = transform(validated)?;
    format_output(transformed)
}

fn parse(data: &[u8]) -> Result<Parsed> { ... }
fn validate(p: Parsed) -> Result<Validated> { ... }
fn transform(v: Validated) -> Result<Transformed> { ... }
fn format_output(t: Transformed) -> Result<Output> { ... }
```

### `#[allow(dead_code)]` Everywhere

```rust
// Anti-pattern: Silencing warnings instead of fixing
#[allow(dead_code)]
fn unused_function() { ... }

#[allow(dead_code)]
struct UnusedStruct { ... }

// Better: Remove unused code or make it pub if it's part of API
// Or use cfg for platform-specific code
#[cfg(target_os = "linux")]
fn linux_only_function() { ... }
```

### Overuse of Strings

```rust
// Anti-pattern: Strings for everything
fn parse_command(cmd: &str) -> String {
    match cmd {
        "start" => "starting".to_string(),
        "stop" => "stopping".to_string(),
        _ => "unknown".to_string(),
    }
}

// Better: Use enums
enum Command {
    Start,
    Stop,
    Unknown,
}

enum Status {
    Starting,
    Stopping,
    Unknown,
}

fn parse_command(cmd: &str) -> Command { ... }
fn execute(cmd: Command) -> Status { ... }
```

## Best Practices Summary

1. **Use the type system** - Encode invariants in types
2. **Prefer borrowing** - Use references instead of cloning
3. **Handle errors properly** - Don't unwrap in library code
4. **Use iterators** - They're zero-cost and composable
5. **Keep functions small** - Single responsibility principle
6. **Document public APIs** - Especially unsafe code
7. **Use traits for abstraction** - Not inheritance
8. **Test your code** - Unit tests, integration tests, doc tests
9. **Use cargo clippy** - Catches many anti-patterns
10. **Keep dependencies minimal** - Only add what you need
