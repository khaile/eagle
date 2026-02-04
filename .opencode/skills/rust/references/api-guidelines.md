# API Design Guidelines

Comprehensive guidelines for designing idiomatic Rust APIs based on the official Rust API Guidelines.

## Naming Conventions (C-CASE)

### Casing Rules (RFC 430)

| Item | Convention | Example |
|------|------------|---------|
| Crates | `snake_case` | `my_crate` |
| Modules | `snake_case` | `my_module` |
| Types | `UpperCamelCase` | `MyStruct` |
| Traits | `UpperCamelCase` | `MyTrait` |
| Enum variants | `UpperCamelCase` | `MyVariant` |
| Functions | `snake_case` | `my_function` |
| Methods | `snake_case` | `do_something` |
| General constructors | `new` or `with_*` | `new()`, `with_capacity()` |
| Conversion constructors | `from_*` | `from_bytes()` |
| Macros | `snake_case!` | `vec!`, `println!` |
| Local variables | `snake_case` | `my_var` |
| Statics | `SCREAMING_SNAKE_CASE` | `MAX_SIZE` |
| Constants | `SCREAMING_SNAKE_CASE` | `PI` |
| Type parameters | `UpperCamelCase`, single letter | `T`, `MyType` |
| Lifetimes | `'lowercase`, short | `'a`, `'static` |

### Conversion Methods (C-CONV)

```rust
// as_*: Free or cheap reference-to-reference conversion
fn as_bytes(&self) -> &[u8];
fn as_mut_slice(&mut self) -> &mut [T];

// to_*: Expensive reference-to-value conversion
fn to_string(&self) -> String;
fn to_vec(&self) -> Vec<T>;

// into_*: Consuming conversion (takes self by value)
fn into_bytes(self) -> Vec<u8>;
fn into_inner(self) -> T;
```

### Getter Methods (C-GETTER)

```rust
// Good: No get_ prefix
impl Widget {
    fn size(&self) -> Size { ... }
    fn color(&self) -> Color { ... }
}

// Bad: Unnecessary get_ prefix
impl Widget {
    fn get_size(&self) -> Size { ... }  // Don't do this
}

// Exception: When there's a collision or ambiguity
impl Config {
    fn value(&self) -> &str { ... }      // The value itself
    fn get_value(&self, key: &str) -> Option<&str> { ... }  // Get by key
}
```

### Iterator Methods (C-ITER)

```rust
impl MyCollection {
    // Immutable iterator
    fn iter(&self) -> Iter<'_, T> { ... }

    // Mutable iterator
    fn iter_mut(&mut self) -> IterMut<'_, T> { ... }

    // Consuming iterator
    fn into_iter(self) -> IntoIter<T> { ... }
}

// Iterator type names should match the method
pub struct Iter<'a, T> { ... }
pub struct IterMut<'a, T> { ... }
pub struct IntoIter<T> { ... }
```

## Interoperability

### Common Traits (C-COMMON-TRAITS)

Implement eagerly where appropriate:

```rust
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Person {
    pub name: String,
    pub age: u32,
}
```

**Guidelines:**
- `Copy`: For types that can be copied bitwise (no heap allocations)
- `Clone`: Nearly all types should implement this
- `Debug`: All public types should implement this
- `Default`: If there's a sensible default value
- `PartialEq`/`Eq`: If values can be compared for equality
- `PartialOrd`/`Ord`: If there's a natural ordering
- `Hash`: If the type can be used as a map key

### Conversion Traits (C-CONV-TRAITS)

```rust
// From/Into: Infallible conversions
impl From<u32> for MyType {
    fn from(value: u32) -> Self { ... }
}
// Into is automatically implemented

// AsRef/AsMut: Cheap reference conversions
impl AsRef<str> for MyString {
    fn as_ref(&self) -> &str { ... }
}

// TryFrom/TryInto: Fallible conversions (Rust 2018+)
impl TryFrom<String> for UserId {
    type Error = ParseError;
    fn try_from(value: String) -> Result<Self, Self::Error> { ... }
}
```

### Collections (C-COLLECT)

```rust
impl FromIterator<T> for MyCollection<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = MyCollection::new();
        c.extend(iter);
        c
    }
}

impl Extend<T> for MyCollection<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for item in iter {
            self.push(item);
        }
    }
}
```

### Send and Sync (C-SEND-SYNC)

```rust
// Most types should be Send and Sync
pub struct SafeType {
    data: Vec<i32>,
}
// Automatically Send + Sync

// Opt out only when necessary
pub struct NotThreadSafe {
    data: Rc<i32>,  // Rc is not Send
}

// Manual implementation (be very careful!)
unsafe impl Send for MyType {}
unsafe impl Sync for MyType {}
```

### Error Types (C-GOOD-ERR)

```rust
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum MyError {
    Io(std::io::Error),
    Parse(ParseError),
    Custom(String),
}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MyError::Io(e) => write!(f, "IO error: {}", e),
            MyError::Parse(e) => write!(f, "Parse error: {}", e),
            MyError::Custom(s) => write!(f, "{}", s),
        }
    }
}

impl Error for MyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            MyError::Io(e) => Some(e),
            MyError::Parse(e) => Some(e),
            MyError::Custom(_) => None,
        }
    }
}
```

## Documentation (C-EXAMPLE, C-FAILURE)

```rust
/// Calculate the factorial of a number.
///
/// # Examples
///
/// ```
/// use mylib::factorial;
///
/// assert_eq!(factorial(5), 120);
/// ```
///
/// # Errors
///
/// Returns `Err` if the number is too large to compute.
///
/// # Panics
///
/// Panics if the input is negative.
///
/// # Safety
///
/// This function is unsafe because it dereferences a raw pointer.
pub fn factorial(n: u64) -> Result<u64, Error> {
    // Implementation
}
```

## Predictability

### Methods vs Functions (C-METHOD)

```rust
// Good: Clear receiver, use method
impl Rectangle {
    pub fn area(&self) -> u32 { ... }
}

// Bad: Should be a method
pub fn rectangle_area(rect: &Rectangle) -> u32 { ... }

// Good: No clear receiver, use function
pub fn parse_config(input: &str) -> Config { ... }
```

### No Out Parameters (C-NO-OUT)

```rust
// Bad: Out parameter
fn parse_number(input: &str, output: &mut i32) -> bool { ... }

// Good: Return value
fn parse_number(input: &str) -> Option<i32> { ... }

// Good: Return multiple values with tuple
fn split_name(full: &str) -> (String, String) { ... }
```

### Deref Only for Smart Pointers (C-DEREF)

```rust
// Good: Smart pointer implements Deref
impl<T> Deref for Box<T> {
    type Target = T;
    fn deref(&self) -> &T { ... }
}

// Bad: Non-smart-pointer implementing Deref
impl Deref for MyString {  // Don't do this!
    type Target = str;
    fn deref(&self) -> &str { ... }
}
// Use AsRef instead for non-smart-pointers
```

### Constructors (C-CTOR)

```rust
impl Widget {
    // Standard constructor
    pub fn new() -> Self { ... }

    // Constructor with parameters
    pub fn with_capacity(cap: usize) -> Self { ... }

    // Conversion constructor
    pub fn from_config(config: Config) -> Self { ... }
}
```

## Type Safety

### Newtypes (C-NEWTYPE)

```rust
// Create distinct types for different concepts
pub struct Meters(f64);
pub struct Seconds(f64);

// Prevents mixing up values
fn velocity(distance: Meters, time: Seconds) -> f64 {
    distance.0 / time.0
}

// Won't compile: type mismatch
// velocity(Seconds(10.0), Meters(5.0));
```

### Custom Types Over Bools (C-CUSTOM-TYPE)

```rust
// Bad: Boolean parameters are unclear
widget.set_hidden(true);
widget.set_enabled(false);

// Good: Custom types make intent clear
pub enum Visibility {
    Visible,
    Hidden,
}

pub enum State {
    Enabled,
    Disabled,
}

widget.set_visibility(Visibility::Hidden);
widget.set_state(State::Disabled);
```

### Bitflags (C-BITFLAG)

```rust
use bitflags::bitflags;

bitflags! {
    pub struct Permissions: u32 {
        const READ = 0b001;
        const WRITE = 0b010;
        const EXECUTE = 0b100;
    }
}

// Usage
let perms = Permissions::READ | Permissions::WRITE;
if perms.contains(Permissions::READ) { ... }
```

### Builder Pattern (C-BUILDER)

```rust
pub struct Config {
    host: String,
    port: u16,
    timeout: Duration,
}

pub struct ConfigBuilder {
    host: Option<String>,
    port: Option<u16>,
    timeout: Option<Duration>,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self { host: None, port: None, timeout: None }
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn build(self) -> Result<Config, Error> {
        Ok(Config {
            host: self.host.ok_or(Error::MissingHost)?,
            port: self.port.unwrap_or(8080),
            timeout: self.timeout.unwrap_or(Duration::from_secs(30)),
        })
    }
}

// Usage
let config = ConfigBuilder::new()
    .host("localhost")
    .port(3000)
    .build()?;
```

## Flexibility

### Generic Parameters (C-GENERIC)

```rust
// Bad: Too specific
fn print_string(s: String) { println!("{}", s); }

// Good: Generic over AsRef<str>
fn print_string(s: impl AsRef<str>) {
    println!("{}", s.as_ref());
}

// Even better: Generic parameter
fn print_string<S: AsRef<str>>(s: S) {
    println!("{}", s.as_ref());
}

// Works with &str, String, Cow<str>, etc.
```

### Object Safety (C-OBJECT)

```rust
// Object-safe trait (can use as trait object)
pub trait Draw {
    fn draw(&self);
}

// Not object-safe (generic method)
pub trait Convert {
    fn convert<T>(&self) -> T;  // Can't be a trait object
}

// Make trait object-safe when useful
pub trait Reader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    // No generic methods, no associated types with generics
}

// Can use as: Box<dyn Reader>
```

## Dependability

### Validate Arguments (C-VALIDATE)

```rust
pub fn take_first(data: &[u8], n: usize) -> &[u8] {
    assert!(n <= data.len(), "n exceeds data length");
    &data[..n]
}

// Or return Result for recoverable errors
pub fn take_first_safe(data: &[u8], n: usize) -> Result<&[u8], Error> {
    if n > data.len() {
        return Err(Error::IndexOutOfBounds);
    }
    Ok(&data[..n])
}
```

### Destructors (C-DTOR-FAIL, C-DTOR-BLOCK)

```rust
// Good: Destructor cannot fail
impl Drop for MyType {
    fn drop(&mut self) {
        // Ignore errors or log them
        let _ = self.cleanup();
    }
}

// Good: Provide explicit cleanup method for blocking operations
impl Connection {
    pub fn close(self) -> Result<(), Error> {
        // Blocking cleanup that can fail
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Best-effort non-blocking cleanup
        let _ = self.try_close_nonblocking();
    }
}
```

## Future Proofing

### Sealed Traits (C-SEALED)

```rust
mod private {
    pub trait Sealed {}
}

// Prevent external implementations
pub trait MyTrait: private::Sealed {
    fn method(&self);
}

impl private::Sealed for MyType {}
impl MyTrait for MyType {
    fn method(&self) { ... }
}
```

### Private Fields (C-STRUCT-PRIVATE)

```rust
// Good: Can add fields without breaking changes
pub struct Config {
    pub timeout: Duration,
    // Private field, can be changed freely
    cache_size: usize,
}

// Bad: All public, can't add fields
pub struct BadConfig {
    pub timeout: Duration,
    pub cache_size: usize,
}
```

### Avoid Trait Bounds on Struct (C-STRUCT-BOUNDS)

```rust
// Bad: Bounds on struct definition
struct Container<T: Clone + Debug> {
    item: T,
}

// Good: Bounds only on impl blocks
struct Container<T> {
    item: T,
}

impl<T: Clone> Container<T> {
    fn duplicate(&self) -> T {
        self.item.clone()
    }
}

impl<T: Debug> Container<T> {
    fn debug(&self) {
        println!("{:?}", self.item);
    }
}
```
