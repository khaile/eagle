# Convenience commands for eagle
# See https://github.com/casey/just
#
# Usage: just <command>
#
# Commands:

alias b := build
alias bm := benchmark
alias c := clean
alias t := test
alias ts := tests
alias s := server
alias rl := rust-lint
alias lc := lint-check
alias mdlint := markdownlint
alias nt := nextest
alias nts := nextests
alias rb := run-benches

clean:
    cargo clean

check:
    cargo check --workspace --all-features --all-targets

rust-lint:
    cargo fmt --all
    just check
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# CI-friendly lint check (doesn't modify files)
lint-check:
    cargo fmt --all -- --check
    just check
    cargo clippy --workspace --all-targets --all-features -- -D warnings

build:
    cargo build --workspace

test:
    just build
    cargo test --workspace --all-features

tests TEST:
    just build
    cargo test --workspace {{ TEST }}

nextest:
    just build
    cargo nextest run --workspace

nextests TEST:
    just build
    cargo nextest run --workspace --nocapture -- {{ TEST }}

benchmark BENCH:
    cargo bench -p eagle-core --bench {{ BENCH }}

server:
    cargo run -p eagle -- --port 6380

run-benches:
    ./scripts/run-benches.sh

markdownlint:
    markdownlint '**/*.md' --ignore-path .gitignore --ignore AGENTS.md
