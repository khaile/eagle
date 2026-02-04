---
name: rust
description: Rust expert specializing in ownership, lifetimes, borrowing, and concurrency. Use when helping users implement safe concurrent data structures, debug borrow checker errors, understand lifetime annotations, resolve Send/Sync trait issues, implement thread-safe code with Arc/Mutex/RwLock, work with atomics and memory ordering, or explain complex ownership patterns. Triggers include questions about borrow checker errors, lifetime annotations, thread safety, data races, concurrent programming, and core Rust memory safety concepts.
---

# Rust Expert

Expert guidance on Rust's core memory safety features: ownership, borrowing, lifetimes, and concurrency.

## When to Use This Skill

Use this skill when the user needs help with:

- Implementing safe concurrent data structures
- Debugging borrow checker errors
- Understanding and applying lifetime annotations
- Resolving Send/Sync trait constraints
- Choosing between Arc, Mutex, RwLock, atomics
- Understanding memory ordering and atomics
- Designing thread-safe APIs
- Converting single-threaded code to concurrent code

## How to Use This Skill

This skill provides comprehensive reference documents covering Rust's core concepts:

### Core Concepts
1. **ownership.md** - Ownership, borrowing, moves, and references
2. **lifetimes.md** - Lifetime annotations, elision, bounds, and common patterns
3. **concurrency.md** - Thread safety, Send/Sync, atomics, locks, and synchronization

### Design & Best Practices
4. **api-guidelines.md** - API design, naming conventions, trait implementations
5. **patterns.md** - Design patterns, idioms, and anti-patterns
6. **style-guide.md** - Formatting, naming, and code organization

### Safety & Correctness
7. **undefined-behavior.md** - What constitutes UB and how to avoid it

### Reference Selection Guide

**Read ownership.md when:**
- User has borrow checker errors
- Questions about moves vs copies
- Confusion about `&` vs `&mut`
- Issues with partial moves
- Questions about when data is dropped

**Read lifetimes.md when:**
- Compiler requires lifetime annotations
- Questions about `'a`, `'static`, `'_`
- Struct contains references
- Lifetime bounds errors
- Questions about lifetime elision rules

**Read concurrency.md when:**
- Implementing multi-threaded code
- Questions about Arc, Mutex, RwLock
- Send/Sync trait errors
- Questions about atomics and ordering
- Need to share mutable state between threads
- Questions about thread safety patterns

**Read api-guidelines.md when:**
- Designing public APIs
- Questions about naming conventions
- Choosing which traits to implement
- Structuring a library
- Making APIs ergonomic and idiomatic

**Read patterns.md when:**
- Looking for common Rust patterns
- Structuring code idiomatically
- Avoiding anti-patterns
- Learning Rust best practices
- Refactoring code

**Read style-guide.md when:**
- Questions about formatting
- Code organization
- Comment conventions
- Import ordering
- Cargo.toml structure

**Read undefined-behavior.md when:**
- Working with unsafe code
- Questions about what's allowed in unsafe
- Debugging mysterious issues
- Validating unsafe abstractions
- Understanding soundness requirements

### Workflow

1. **Identify the problem area** from user's question or error message
2. **Read the relevant reference(s)** using the guide above
3. **Provide targeted explanation** using concepts from the references
4. **Show code examples** that demonstrate the solution
5. **Explain trade-offs** when multiple approaches exist

## Key Principles

### Teaching Approach

- Start with the user's specific error or question
- Reference official Rust terminology from the documentation
- Provide runnable code examples
- Explain the "why" behind Rust's rules, not just the "how"
- Offer multiple solutions when appropriate, explaining trade-offs

### Code Quality

When providing solutions:
- Prefer idiomatic Rust patterns
- Use appropriate synchronization primitives (don't over-synchronize)
- Consider both correctness and performance
- Minimize lock contention
- Use the type system to encode invariants

### Common Patterns

Address these common scenarios with established patterns:

**Sharing immutable data**: `Arc<T>`
**Sharing mutable data**: `Arc<Mutex<T>>` or `Arc<RwLock<T>>`
**Single values with lock-free updates**: Atomics
**Message passing**: Channels (`mpsc`)
**One-time initialization**: `Once` or `OnceLock`

## Explaining Errors

### Borrow Checker Errors

When user shares borrow checker errors:

1. Identify the specific rule being violated
2. Explain which references exist and their types
3. Show why the borrow checker prevents this
4. Provide 2-3 alternative approaches
5. Recommend the most idiomatic solution

### Lifetime Errors

When user shares lifetime errors:

1. Explain what the lifetime annotation means in context
2. Identify which reference might outlive its data
3. Show the lifetime relationships
4. Provide solution with proper annotations or restructuring

### Send/Sync Errors

When user hits thread safety errors:

1. Explain why the type isn't Send/Sync
2. Show what makes it thread-unsafe
3. Provide thread-safe alternatives
4. Explain performance implications

## Advanced Topics

### When to Use Unsafe

Occasionally the user may need `unsafe`. When appropriate:

- Explain why safe Rust cannot express their requirement
- Show the invariants they must uphold
- Demonstrate proper unsafe boundaries
- Suggest safe alternatives if they exist

### Performance Optimization

When optimizing concurrent code:

- Profile before optimizing
- Consider lock-free alternatives (atomics) for hot paths
- Minimize critical sections
- Batch operations to reduce synchronization
- Consider reader-writer locks for read-heavy workloads

### Complex Lifetime Scenarios

For advanced lifetime cases:

- Higher-ranked trait bounds (HRTB)
- Lifetime bounds on types
- Self-referential structs (usually need Pin)
- Variance in generic parameters

## Resources Reference

All detailed technical information is in the reference documents:

**Core Concepts:**
- `references/ownership.md`
- `references/lifetimes.md`
- `references/concurrency.md`

**Design & Best Practices:**
- `references/api-guidelines.md`
- `references/patterns.md`
- `references/style-guide.md`

**Safety:**
- `references/undefined-behavior.md`

Read these documents to provide accurate, detailed guidance on specific topics.
