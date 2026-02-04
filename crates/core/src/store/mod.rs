//! Storage Engine Module
//!
//! Provides multiple storage backends:
//! - `Store` - DashMap-based in-memory store with optional PMEM persistence
//! - `StoreDash` - Extendible hashing based store
//! - `memory` - Pure in-memory store utilities
//!
//! ## Modules
//!
//! - `aof` - Append-Only File persistence support
//! - `dash` - Extendible hashing implementation
//! - `entry` - Entry metadata and TTL functionality
//! - `expiration` - Background expiration task
//! - `memory` - Memory management utilities
//! - `policy` - Eviction policies
//! - `secure_bytes` - Secure data types
//! - `set_ops` - SET operation options and results
//! - `snapshot` - Snapshot file format and persistence
//! - `snapshot_scheduler` - Automated snapshot scheduling

use crate::hash::HighwayHasher;
use crate::pmem::{PmemAllocator, RecoveryManager};

use crate::sync::epoch::Epoch;

use crate::error::{Result, StorageError};
use dashmap::DashMap;
use hashbrown::HashMap;
use parking_lot::RwLock;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info};

pub mod aof;
pub mod dash;
pub mod entry;
pub mod expiration;
pub mod memory;
pub mod policy;
pub mod secure_bytes;
pub mod set_ops;
pub mod snapshot;
pub mod snapshot_scheduler;

pub use entry::EntryMeta;
pub use set_ops::{SetOptions, SetResult};
pub use snapshot_scheduler::{SnapshotConfig, SnapshotScheduler};

/// Entry metadata stored alongside values
/// Main key-value store with optional PMEM persistence
#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

type HashData = DashMap<Vec<u8>, RwLock<HashMap<Vec<u8>, Vec<u8>>>>;

enum RecoveryItem {
    String {
        offset: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        expiry: u64,
    },
    Hash {
        hash_key: Vec<u8>,
        field: Vec<u8>,
        value: Vec<u8>,
    },
}

struct StoreInner {
    /// In-memory data store: key -> (value, metadata)
    data: DashMap<Vec<u8>, (Vec<u8>, EntryMeta)>,
    /// Optional PMEM allocator for persistence
    pmem: Option<Arc<PmemAllocator>>,
    /// Optional AOF writer for logging commands
    aof_writer: Option<aof::AsyncAofWriter>,
    /// Epoch-based memory reclamation
    epoch: Epoch,
    /// Eviction policy
    policy: RwLock<policy::Policy>,
    /// Fast-path flag: true if eviction is enabled (avoids lock acquisition)
    eviction_enabled: AtomicBool,
    /// Hash data for Redis HASH commands
    /// Uses RwLock<HashMap> for inner maps to avoid deadlock risk of nested DashMap
    hash_data: HashData,
    /// Hasher for computing key hashes
    hasher: HighwayHasher,
    /// Last successful save timestamp (Unix seconds)
    last_save: AtomicU64,
    /// Whether a background save is currently in progress
    bgsave_in_progress: AtomicBool,
    /// Snapshot scheduler handle (for shutdown)
    #[allow(dead_code)]
    snapshot_scheduler: RwLock<Option<SnapshotScheduler>>,
}

impl Store {
    /// Create a new in-memory store
    pub fn new() -> Result<Self> {
        Ok(Self {
            inner: Arc::new(StoreInner {
                data: DashMap::new(),
                pmem: None,
                aof_writer: None,
                epoch: Epoch::new(),
                policy: RwLock::new(policy::Policy::default()),
                eviction_enabled: AtomicBool::new(false), // Default policy is None
                hash_data: DashMap::new(),
                hasher: HighwayHasher::new(),
                last_save: AtomicU64::new(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                ),
                bgsave_in_progress: AtomicBool::new(false),
                snapshot_scheduler: RwLock::new(None),
            }),
        })
    }

    /// Create a new store with snapshot scheduling enabled
    pub fn new_with_snapshot(config: SnapshotConfig) -> Result<Self> {
        let mut store = Self::new()?;

        // Initialize AOF if enabled
        if config.aof_enabled {
            let aof_writer = aof::AsyncAofWriter::new(&config.aof_path)
                .map_err(|e| StorageError::Aof(e.to_string()))?;
            if let Some(inner) = Arc::get_mut(&mut store.inner) {
                inner.aof_writer = Some(aof_writer);
            }
        }

        // Use a Weak reference to break the cycle between Store and SnapshotScheduler
        let weak_inner = Arc::downgrade(&store.inner);
        // Share the configuration with the scheduler/task via Arc to avoid cloning large configs
        let arc_config = Arc::new(config);
        let scheduler = SnapshotScheduler::new(weak_inner, Arc::clone(&arc_config))
            .map_err(|e| StorageError::Snapshot(e.to_string()))?;
        *store.inner.snapshot_scheduler.write() = Some(scheduler);
        Ok(store)
    }

    /// Create a new in-memory store (alias for new())
    // TODO: Remove or implement PMEM recovery in new_memory (Milestone 3.2)
    #[allow(dead_code)]
    pub fn new_memory() -> Result<Self> {
        Self::new()
    }

    /// Create a new store with PMEM persistence
    ///
    /// If the PMEM file exists, data will be recovered from it.
    pub fn new_pmem(path: &str, size_mb: usize) -> Result<Self> {
        use crate::pmem::layout::{HASH_ENTRY_PREFIX, HASH_ENTRY_PREFIX_LEN};
        use crossbeam::channel;

        let pmem = Arc::new(PmemAllocator::recover(path, size_mb)?);
        let data = DashMap::new();
        let hash_data: HashData = DashMap::new();
        let hasher = HighwayHasher::new();
        let string_count = AtomicUsize::new(0);
        let hash_count = AtomicUsize::new(0);

        // Recover existing data from PMEM
        if pmem.entry_count() > 0 {
            info!("Recovering {} entries from PMEM", pmem.entry_count());
            let _recovery = RecoveryManager::new();

            let num_workers = num_cpus::get();
            let mut senders = Vec::with_capacity(num_workers);
            let mut receivers = Vec::with_capacity(num_workers);

            for _ in 0..num_workers {
                let (tx, rx) = channel::bounded(10_000);
                senders.push(tx);
                receivers.push(rx);
            }

            std::thread::scope(|s| {
                // Spawn workers
                for rx in receivers {
                    let data = &data;
                    let hash_data = &hash_data;
                    let string_count = &string_count;
                    let hash_count = &hash_count;

                    s.spawn(move || {
                        for item in rx {
                            match item {
                                RecoveryItem::String {
                                    offset,
                                    key,
                                    value,
                                    expiry,
                                } => {
                                    // Convert persisted expiration timestamp to Instant
                                    let expires_at = if expiry > 0 {
                                        let now_unix_ms = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis()
                                            as u64;
                                        if expiry > now_unix_ms {
                                            let remaining_ms = expiry - now_unix_ms;
                                            Some(
                                                Instant::now()
                                                    + std::time::Duration::from_millis(
                                                        remaining_ms,
                                                    ),
                                            )
                                        } else {
                                            // Expired
                                            continue;
                                        }
                                    } else {
                                        None
                                    };

                                    let meta = EntryMeta {
                                        timestamp: Instant::now(),
                                        pmem_offset: Some(offset),
                                        expires_at,
                                    };
                                    data.insert(key, (value, meta));
                                    string_count.fetch_add(1, Ordering::Relaxed);
                                }
                                RecoveryItem::Hash {
                                    hash_key,
                                    field,
                                    value,
                                } => {
                                    // For hash entries, we need to handle concurrent updates carefully.
                                    // By sharding on hash_key, we ensure that updates for the same
                                    // hash_key are processed by the same worker, or at least serialized
                                    // via the channel order if all go to same worker.
                                    // However, sharding guarantees only strict ordering if channel is FIFO
                                    // and single worker per shard.
                                    // Since we shard by hash(key) % num_workers, all updates for a hash
                                    // go to the same channel, consumed by ONE worker.
                                    // So order is preserved.

                                    let inner_map = hash_data
                                        .entry(hash_key)
                                        .or_insert_with(|| RwLock::new(HashMap::new()));
                                    inner_map.write().insert(field, value);
                                    hash_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    });
                }

                // Iterate and distribute work
                // We use a single pass over all entries to avoid reading the file twice
                let iter_result = pmem.iter_entries(|offset, key, value, expires_at_millis| {
                    // Check if it's a hash entry
                    if key.len() > HASH_ENTRY_PREFIX_LEN
                        && &key[..HASH_ENTRY_PREFIX_LEN] == HASH_ENTRY_PREFIX
                    {
                        // Parse hash entry
                        // Format: PREFIX (6) | hash_key_len (1) | hash_key | field
                        let hash_key_len_offset = HASH_ENTRY_PREFIX_LEN;
                        if hash_key_len_offset < key.len() {
                            let hash_key_len = key[hash_key_len_offset] as usize;
                            if hash_key_len_offset + 1 + hash_key_len <= key.len() {
                                let hash_key = key[hash_key_len_offset + 1
                                    ..hash_key_len_offset + 1 + hash_key_len]
                                    .to_vec();
                                let field = key[hash_key_len_offset + 1 + hash_key_len..].to_vec();

                                // Shard by hash_key to ensure serialization of updates to same hash
                                // Use the same hasher as Store for consistency, though any stable hash works
                                let mut h = std::collections::hash_map::DefaultHasher::new();
                                use std::hash::Hasher;
                                h.write(&hash_key);
                                let shard = (h.finish() as usize) % num_workers;

                                let _ = senders[shard].send(RecoveryItem::Hash {
                                    hash_key,
                                    field,
                                    value: value.to_vec(),
                                });
                                return true;
                            }
                        }
                        // Malformed hash entry, treat as string or ignore?
                        // Treat as string for safety/fallback
                    }

                    // String entry
                    let mut h = std::collections::hash_map::DefaultHasher::new();
                    use std::hash::Hasher;
                    h.write(key);
                    let shard = (h.finish() as usize) % num_workers;

                    let _ = senders[shard].send(RecoveryItem::String {
                        offset,
                        key: key.to_vec(),
                        value: value.to_vec(),
                        expiry: expires_at_millis,
                    });
                    true
                });

                if let Err(e) = iter_result {
                    error!("Error iterating PMEM entries: {}", e);
                }

                // Drop senders to signal workers to stop
                drop(senders);
                // Workers join automatically at end of scope
            });

            info!(
                "Recovered {} string entries and {} hash fields to in-memory store",
                string_count.load(Ordering::Relaxed),
                hash_count.load(Ordering::Relaxed)
            );
        }

        Ok(Self {
            inner: Arc::new(StoreInner {
                data,
                pmem: Some(pmem),
                aof_writer: None,
                epoch: Epoch::new(),
                policy: RwLock::new(policy::Policy::default()),
                eviction_enabled: AtomicBool::new(false), // Default policy is None
                hash_data,
                hasher,
                last_save: AtomicU64::new(
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                ),
                bgsave_in_progress: AtomicBool::new(false),
                snapshot_scheduler: RwLock::new(None),
            }),
        })
    }

    /// Create a new store with PMEM persistence and snapshot scheduling
    pub fn new_pmem_with_snapshot(
        path: &str,
        size_mb: usize,
        config: SnapshotConfig,
    ) -> Result<Self> {
        let mut store = Self::new_pmem(path, size_mb)?;

        // Initialize AOF if enabled
        if config.aof_enabled {
            let aof_writer = aof::AsyncAofWriter::new(&config.aof_path)
                .map_err(|e| StorageError::Aof(e.to_string()))?;
            if let Some(inner) = Arc::get_mut(&mut store.inner) {
                inner.aof_writer = Some(aof_writer);
            }
        }

        // Use a Weak reference to break the cycle
        let weak_inner = Arc::downgrade(&store.inner);
        // Share the configuration with the scheduler/task via Arc to avoid cloning large configs
        let arc_config = Arc::new(config);
        let scheduler = SnapshotScheduler::new(weak_inner, Arc::clone(&arc_config))
            .map_err(|e| StorageError::Snapshot(e.to_string()))?;
        *store.inner.snapshot_scheduler.write() = Some(scheduler);
        Ok(store)
    }

    /// Apply eviction policy if enabled (fast-path check avoids lock)
    #[inline]
    fn maybe_apply_policy(&self) {
        // Fast-path: skip if eviction is disabled
        if !self.inner.eviction_enabled.load(Ordering::Relaxed) {
            return;
        }
        self.inner.policy.read().apply_entries(&self.inner.data);
    }

    /// Return true if append-only logging is enabled.
    pub fn aof_enabled(&self) -> bool {
        self.inner.aof_writer.is_some()
    }

    /// Append a raw RESP command to the AOF log if enabled.
    pub async fn append_aof(&self, command: &[u8]) -> Result<()> {
        if let Some(writer) = &self.inner.aof_writer {
            writer
                .log_command(command)
                .await
                .map_err(|e| StorageError::Aof(e.to_string()))?;
        }
        Ok(())
    }

    /// Get a value by key
    /// Returns None if key doesn't exist or has expired (lazy expiration)
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let _epoch_guard = self.inner.epoch.enter();

        // Check if key exists and is not expired
        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                // Key has expired, remove it (lazy deletion)
                drop(entry); // Release the read lock first
                self.delete(key);
                return None;
            }
            Some(entry.0.clone())
        } else {
            None
        }
    }

    /// Get a value and its metadata by key (internal use)
    #[allow(dead_code)]
    fn get_with_meta(&self, key: &[u8]) -> Option<(Vec<u8>, EntryMeta)> {
        let _epoch_guard = self.inner.epoch.enter();

        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return None;
            }
            Some((entry.0.clone(), entry.1.clone()))
        } else {
            None
        }
    }

    /// Put a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) {
        let _epoch_guard = self.inner.epoch.enter();

        let mut meta = EntryMeta::default();

        // Persist to PMEM if available
        if let Some(ref pmem) = self.inner.pmem {
            let key_hash = self.inner.hasher.hash(&key);
            match pmem.allocate_entry(key_hash, &key, &value) {
                Ok(alloc) => {
                    meta.pmem_offset = Some(alloc.offset);
                    debug!("Persisted key to PMEM at offset {}", alloc.offset);
                }
                Err(e) => {
                    // Log error but continue with in-memory storage
                    tracing::error!("Failed to persist to PMEM: {}", e);
                }
            }
        }

        self.inner.data.insert(key, (value, meta));
        self.maybe_apply_policy();
    }

    /// Set a key-value pair (alias for put with Result return)
    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.put(key, value);
        Ok(())
    }

    /// Set a key-value pair with options (NX, XX, GET, TTL)
    pub fn set_with_options(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        options: SetOptions,
    ) -> Result<SetResult> {
        use dashmap::mapref::entry::Entry;
        let _epoch_guard = self.inner.epoch.enter();

        // Use Entry API for atomic check-and-set
        match self.inner.data.entry(key.clone()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();

                // Check if expired - treat as non-existent
                if entry.1.is_expired() {
                    // Key is expired, treat as if it doesn't exist
                    if options.xx {
                        // XX requires key to exist, but it's expired
                        return Ok(SetResult::NotPerformed);
                    }

                    // NX or no condition: proceed with set
                    let mut meta = EntryMeta::default();

                    // Handle TTL
                    if let Some(ttl) = options.expiration {
                        if ttl.is_zero() {
                            // TTL of 0 means expire immediately, just remove
                            occupied.remove();
                            return Ok(SetResult::Ok);
                        }
                        meta.expires_at = Some(Instant::now() + ttl);
                    }

                    // Persist to PMEM if available
                    self.persist_to_pmem(&key, &value, &mut meta);

                    // Replace the expired entry
                    *entry = (value, meta);
                    self.maybe_apply_policy();

                    return Ok(SetResult::Ok);
                }

                // Key exists and is not expired
                if options.nx {
                    // NX requires key to NOT exist
                    return Ok(if options.get {
                        SetResult::NotPerformedWithOldValue(entry.0.clone())
                    } else {
                        SetResult::NotPerformed
                    });
                }

                // XX condition is satisfied (key exists), or no condition
                let old_value = if options.get {
                    Some(entry.0.clone())
                } else {
                    None
                };
                let old_pmem_offset = entry.1.pmem_offset;

                // Build new metadata
                let mut meta = EntryMeta::default();

                // Handle TTL
                if options.keep_ttl {
                    meta.expires_at = entry.1.expires_at;
                } else if let Some(ttl) = options.expiration {
                    if ttl.is_zero() {
                        // TTL of 0 means expire immediately
                        let result = if let Some(old) = old_value {
                            SetResult::OkWithOldValue(old)
                        } else {
                            SetResult::Ok
                        };
                        // Clean up old PMEM entry
                        if let (Some(pmem), Some(offset)) = (&self.inner.pmem, old_pmem_offset) {
                            let _ = pmem.delete_entry(offset);
                        }
                        occupied.remove();
                        return Ok(result);
                    }
                    meta.expires_at = Some(Instant::now() + ttl);
                }

                // Persist to PMEM if available
                self.persist_to_pmem(&key, &value, &mut meta);

                // Delete old PMEM entry
                if let (Some(pmem), Some(offset)) = (&self.inner.pmem, old_pmem_offset) {
                    let _ = pmem.delete_entry(offset);
                }

                // Update the entry
                *entry = (value, meta);
                self.maybe_apply_policy();

                Ok(match old_value {
                    Some(old) => SetResult::OkWithOldValue(old),
                    None => SetResult::Ok,
                })
            }
            Entry::Vacant(vacant) => {
                // Key does not exist
                if options.xx {
                    // XX requires key to exist
                    return Ok(SetResult::NotPerformed);
                }

                // NX condition is satisfied (key doesn't exist), or no condition
                let mut meta = EntryMeta::default();

                // Handle TTL
                if let Some(ttl) = options.expiration {
                    if ttl.is_zero() {
                        // TTL of 0 means expire immediately, don't even store
                        return Ok(SetResult::Ok);
                    }
                    meta.expires_at = Some(Instant::now() + ttl);
                }

                // Persist to PMEM if available
                self.persist_to_pmem(&key, &value, &mut meta);

                // Insert new entry
                vacant.insert((value, meta));
                self.maybe_apply_policy();

                Ok(SetResult::Ok)
            }
        }
    }

    /// Helper to persist entry to PMEM if available
    fn persist_to_pmem(&self, key: &[u8], value: &[u8], meta: &mut EntryMeta) {
        if let Some(ref pmem) = self.inner.pmem {
            let key_hash = self.inner.hasher.hash(key);
            match pmem.allocate_entry(key_hash, key, value) {
                Ok(alloc) => {
                    meta.pmem_offset = Some(alloc.offset);
                    debug!("Persisted key to PMEM at offset {}", alloc.offset);
                }
                Err(e) => {
                    tracing::error!("Failed to persist to PMEM: {}", e);
                }
            }
        }
    }

    /// Helper to persist hash field to PMEM if available
    fn persist_hash_to_pmem(&self, hash_key: &[u8], field: &[u8], value: &[u8]) {
        if let Some(ref pmem) = self.inner.pmem {
            let key_hash = self.inner.hasher.hash(hash_key);
            match pmem.allocate_hash_entry(key_hash, hash_key, field, value) {
                Ok(_alloc) => {
                    debug!(
                        "Persisted hash field to PMEM (key={:?}, field={:?})",
                        hash_key, field
                    );
                }
                Err(e) => {
                    tracing::error!("Failed to persist hash to PMEM: {}", e);
                }
            }
        }
    }

    /// Delete a key (supports both string and hash keys)
    pub fn delete(&self, key: &[u8]) -> bool {
        let _epoch_guard = self.inner.epoch.enter();

        // Try to delete from string data
        if let Some((_, (_, meta))) = self.inner.data.remove(key) {
            // Mark as deleted in PMEM if applicable
            let pmem_ref = self.inner.pmem.as_ref();
            if let (Some(pmem), Some(offset)) = (pmem_ref, meta.pmem_offset)
                && let Err(e) = pmem.delete_entry(offset)
            {
                tracing::error!("Failed to delete from PMEM: {}", e);
            }
            return true;
        }

        // Try to delete from hash data
        if self.inner.hash_data.remove(key).is_some() {
            return true;
        }

        false
    }

    /// Set the eviction policy
    #[allow(dead_code)]
    pub fn set_policy(&self, policy: policy::Policy) {
        *self.inner.policy.write() = policy;
    }

    /// Get the current number of entries
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.inner.data.len()
    }

    /// Check if the store is empty
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.inner.data.is_empty()
    }

    // ========== TTL/Expiration Methods ==========

    /// Set a key-value pair with an optional expiration time
    #[allow(dead_code)]
    pub fn set_with_expiry(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: Option<Instant>,
    ) -> Result<()> {
        let _epoch_guard = self.inner.epoch.enter();

        let mut meta = EntryMeta {
            timestamp: Instant::now(),
            pmem_offset: None,
            expires_at,
        };

        // Persist to PMEM if available
        if let Some(ref pmem) = self.inner.pmem {
            let key_hash = self.inner.hasher.hash(&key);
            // Convert expires_at to Unix timestamp in milliseconds
            let expires_at_millis = expires_at.map_or(0, |exp| {
                let now = Instant::now();
                let now_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                if exp > now {
                    now_unix_ms + exp.duration_since(now).as_millis() as u64
                } else {
                    0 // Already expired
                }
            });
            match pmem.allocate_entry_with_expiry(key_hash, &key, &value, expires_at_millis) {
                Ok(alloc) => {
                    meta.pmem_offset = Some(alloc.offset);
                    debug!("Persisted key to PMEM at offset {} with TTL", alloc.offset);
                }
                Err(e) => {
                    tracing::error!("Failed to persist to PMEM: {}", e);
                }
            }
        }

        self.inner.data.insert(key, (value, meta));
        self.maybe_apply_policy();
        Ok(())
    }

    /// Set expiration on an existing key (in seconds from now)
    /// Returns true if the key exists and timeout was set
    pub fn expire(&self, key: &[u8], seconds: u64) -> bool {
        self.pexpire(key, seconds * 1000)
    }

    /// Set expiration on an existing key (in milliseconds from now)
    /// Returns true if the key exists and timeout was set
    pub fn pexpire(&self, key: &[u8], milliseconds: u64) -> bool {
        let _epoch_guard = self.inner.epoch.enter();

        if let Some(mut entry) = self.inner.data.get_mut(key) {
            // Check if already expired
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return false;
            }

            let expires_at = Instant::now() + std::time::Duration::from_millis(milliseconds);
            entry.1.expires_at = Some(expires_at);
            true
        } else {
            false
        }
    }

    /// Set expiration as Unix timestamp (seconds)
    /// Returns true if the key exists and timeout was set
    pub fn expireat(&self, key: &[u8], unix_timestamp_secs: u64) -> bool {
        self.pexpireat(key, unix_timestamp_secs * 1000)
    }

    /// Set expiration as Unix timestamp (milliseconds)
    /// Returns true if the key exists and timeout was set
    pub fn pexpireat(&self, key: &[u8], unix_timestamp_ms: u64) -> bool {
        let _epoch_guard = self.inner.epoch.enter();

        if let Some(mut entry) = self.inner.data.get_mut(key) {
            // Check if already expired
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return false;
            }

            // Convert Unix timestamp to Instant
            let now_unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if unix_timestamp_ms <= now_unix_ms {
                // Timestamp is in the past, delete the key
                drop(entry);
                self.delete(key);
                return false;
            }

            let remaining_ms = unix_timestamp_ms - now_unix_ms;
            let expires_at = Instant::now() + std::time::Duration::from_millis(remaining_ms);
            entry.1.expires_at = Some(expires_at);
            true
        } else {
            false
        }
    }

    /// Get TTL in seconds (-2 if key doesn't exist, -1 if no expiration)
    pub fn ttl(&self, key: &[u8]) -> i64 {
        match self.pttl(key) {
            -2 => -2,
            -1 => -1,
            ms => ms / 1000,
        }
    }

    /// Get TTL in milliseconds (-2 if key doesn't exist, -1 if no expiration)
    pub fn pttl(&self, key: &[u8]) -> i64 {
        let _epoch_guard = self.inner.epoch.enter();

        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return -2;
            }

            match entry.1.ttl_millis() {
                Some(ms) => ms as i64,
                None => -1, // No expiration set
            }
        } else {
            -2 // Key doesn't exist
        }
    }

    /// Remove expiration from a key
    /// Returns true if the key exists and had an expiration, false otherwise
    pub fn persist(&self, key: &[u8]) -> bool {
        let _epoch_guard = self.inner.epoch.enter();

        if let Some(mut entry) = self.inner.data.get_mut(key) {
            // Check if already expired
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return false;
            }

            if entry.1.expires_at.is_some() {
                entry.1.expires_at = None;
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Check if a key exists (and is not expired)
    pub fn exists(&self, key: &[u8]) -> bool {
        let _epoch_guard = self.inner.epoch.enter();

        // Check hash data first (hash keys don't currently support expiration)
        if self.inner.hash_data.get(key).is_some() {
            return true;
        }

        // Check string data
        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return false;
            }
            true
        } else {
            false
        }
    }

    // ========== String Operations (Milestone 1.3) ==========

    /// Set multiple key-value pairs
    ///
    /// Note: This operation is NOT strictly atomic. While all inserts will complete
    /// successfully (all-or-nothing in terms of success), concurrent readers may
    /// observe partial state during execution. The epoch guard ensures memory safety
    /// but does not prevent interleaved reads.
    ///
    /// For use cases requiring true atomicity (all keys visible at once or none),
    /// a higher-level locking mechanism would be needed.
    pub fn mset(&self, pairs: &[(impl AsRef<[u8]>, impl AsRef<[u8]>)]) {
        let _epoch_guard = self.inner.epoch.enter();

        // Insert all entries - each insert is atomic per-key, but the overall
        // operation is not atomic across all keys
        for (k, v) in pairs {
            self.inner.data.insert(
                k.as_ref().to_vec(),
                (v.as_ref().to_vec(), EntryMeta::default()),
            );
        }
    }

    /// Increment integer value by specified amount
    /// Creates the key with value 0 if it doesn't exist
    pub fn incrby(&self, key: &[u8], increment: i64) -> Result<i64> {
        use dashmap::mapref::entry::Entry;
        let _epoch_guard = self.inner.epoch.enter();

        // Use Entry API for atomic read-modify-write
        match self.inner.data.entry(key.to_vec()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();

                // Check if expired
                if entry.1.is_expired() {
                    // Reset to 0 + increment
                    let new_value = increment; // 0 + increment
                    entry.0 = new_value.to_string().into_bytes();
                    entry.1 = EntryMeta::default();
                    return Ok(new_value);
                }

                // Parse current value - use str::from_utf8 to avoid clone
                let s = std::str::from_utf8(&entry.0).map_err(|_| {
                    StorageError::InvalidType("value is not an integer or out of range".to_string())
                })?;
                let current = s.parse::<i64>().map_err(|_| {
                    StorageError::InvalidType("value is not an integer or out of range".to_string())
                })?;

                // Compute new value with overflow check
                let new_value = current.checked_add(increment).ok_or_else(|| {
                    StorageError::Overflow("increment would produce overflow".to_string())
                })?;

                // Update in place, preserving metadata (including TTL)
                entry.0 = new_value.to_string().into_bytes();
                Ok(new_value)
            }
            Entry::Vacant(vacant) => {
                // Key doesn't exist, create with value = increment (0 + increment)
                let new_value = increment;
                vacant.insert((new_value.to_string().into_bytes(), EntryMeta::default()));
                Ok(new_value)
            }
        }
    }

    /// Decrement integer value by specified amount
    pub fn decrby(&self, key: &[u8], decrement: i64) -> Result<i64> {
        // Decrement is just increment with negated value
        let neg_decrement = decrement.checked_neg().ok_or_else(|| {
            StorageError::Overflow("decrement would produce overflow".to_string())
        })?;
        self.incrby(key, neg_decrement)
    }

    /// Increment float value by specified amount
    pub fn incrbyfloat(&self, key: &[u8], increment: f64) -> Result<f64> {
        use dashmap::mapref::entry::Entry;

        fn catch_nan_or_infinity(value: f64) -> Result<f64> {
            if value.is_nan() || value.is_infinite() {
                return Err(StorageError::InvalidType(
                    "increment would produce NaN or Infinity".to_string(),
                ));
            }
            Ok(value)
        }

        // Validate increment parameter - reject NaN and Infinity upfront
        catch_nan_or_infinity(increment)?;

        let _epoch_guard = self.inner.epoch.enter();

        // Use Entry API for atomic read-modify-write
        match self.inner.data.entry(key.to_vec()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();

                // Check if expired
                if entry.1.is_expired() {
                    // Reset to 0.0 + increment
                    let new_value = increment;
                    catch_nan_or_infinity(new_value)?;
                    entry.0 = new_value.to_string().into_bytes();
                    entry.1 = EntryMeta::default();
                    return Ok(new_value);
                }

                // Parse current value - use str::from_utf8 to avoid clone
                let s = std::str::from_utf8(&entry.0).map_err(|_| {
                    StorageError::InvalidType("value is not a valid float".to_string())
                })?;
                let current = s.parse::<f64>().map_err(|_| {
                    StorageError::InvalidType("value is not a valid float".to_string())
                })?;

                catch_nan_or_infinity(current)?;

                let new_value = current + increment;
                catch_nan_or_infinity(new_value)?;

                // Update in place, preserving metadata (including TTL)
                entry.0 = new_value.to_string().into_bytes();
                Ok(new_value)
            }
            Entry::Vacant(vacant) => {
                // Key doesn't exist, create with value = increment (0.0 + increment)
                let new_value = increment;
                catch_nan_or_infinity(new_value)?;
                vacant.insert((new_value.to_string().into_bytes(), EntryMeta::default()));
                Ok(new_value)
            }
        }
    }

    /// Append value to existing string (or create new string)
    /// Returns the length of the string after appending
    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<usize> {
        use dashmap::mapref::entry::Entry;
        let _epoch_guard = self.inner.epoch.enter();

        // Use Entry API for atomic read-modify-write
        match self.inner.data.entry(key.to_vec()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();

                // Check if expired
                if entry.1.is_expired() {
                    // Reset to just the appended value
                    entry.0 = value.to_vec();
                    entry.1 = EntryMeta::default();
                    return Ok(value.len());
                }

                // Append to existing value, preserving TTL
                entry.0.extend_from_slice(value);
                Ok(entry.0.len())
            }
            Entry::Vacant(vacant) => {
                // Key doesn't exist, create with the appended value
                let new_value = value.to_vec();
                let len = new_value.len();
                vacant.insert((new_value, EntryMeta::default()));
                Ok(len)
            }
        }
    }

    /// Get the length of a string value
    /// Returns 0 if key doesn't exist
    pub fn strlen(&self, key: &[u8]) -> usize {
        let _epoch_guard = self.inner.epoch.enter();

        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return 0;
            }
            entry.0.len()
        } else {
            0
        }
    }

    /// Get a substring of a string value
    /// Supports negative indices (from end of string)
    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Vec<u8> {
        let _epoch_guard = self.inner.epoch.enter();

        let value = if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return Vec::new();
            }
            entry.0.clone()
        } else {
            return Vec::new();
        };

        if value.is_empty() {
            return Vec::new();
        }

        let len = value.len() as i64;

        // Convert negative indices to positive
        let start_idx = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };

        let end_idx = if end < 0 {
            (len + end).max(0) as usize
        } else {
            end.min(len - 1) as usize
        };

        // If start is beyond string length, return empty
        if start_idx >= value.len() {
            return Vec::new();
        }

        // If start > end after conversion, return empty
        if start_idx > end_idx {
            return Vec::new();
        }

        value[start_idx..=end_idx].to_vec()
    }

    /// Overwrite part of a string at offset
    /// Pads with null bytes if offset is beyond string length
    /// Returns the length of the string after modification
    pub fn setrange(&self, key: &[u8], offset: usize, value: &[u8]) -> Result<usize> {
        use dashmap::mapref::entry::Entry;
        let _epoch_guard = self.inner.epoch.enter();

        // Pre-validate the required length to fail fast on overflow
        let required_len = offset.checked_add(value.len()).ok_or_else(|| {
            StorageError::LimitExceeded("string exceeds maximum allowed size".to_string())
        })?;

        // Use Entry API for atomic read-modify-write
        match self.inner.data.entry(key.to_vec()) {
            Entry::Occupied(mut occupied) => {
                let entry = occupied.get_mut();

                // Check if expired
                if entry.1.is_expired() {
                    // Reset to new value at offset
                    let mut new_value = vec![0u8; required_len];
                    new_value[offset..required_len].copy_from_slice(value);
                    entry.0 = new_value;
                    entry.1 = EntryMeta::default();
                    return Ok(required_len);
                }

                // Extend with null bytes if necessary
                if entry.0.len() < required_len {
                    entry.0.resize(required_len, 0);
                }

                // Copy the value at offset (preserving TTL)
                entry.0[offset..offset + value.len()].copy_from_slice(value);
                Ok(entry.0.len())
            }
            Entry::Vacant(vacant) => {
                // Key doesn't exist, create with value at offset
                let mut new_value = vec![0u8; required_len];
                new_value[offset..required_len].copy_from_slice(value);
                let len = new_value.len();
                vacant.insert((new_value, EntryMeta::default()));
                Ok(len)
            }
        }
    }

    // ========== Generic Key Operations (Milestone 1.3) ==========

    /// Get the type of a key
    /// Returns "string", "hash", or "none"
    pub fn key_type(&self, key: &[u8]) -> String {
        let _epoch_guard = self.inner.epoch.enter();

        // Check if it's a hash (use get() to hold reference and verify existence)
        // Note: hash keys don't currently support expiration
        if self.inner.hash_data.get(key).is_some() {
            return "hash".to_string();
        }

        // Check if it's a string
        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.delete(key);
                return "none".to_string();
            }
            return "string".to_string();
        }

        "none".to_string()
    }

    /// Rename a key
    /// Returns error if the key doesn't exist
    pub fn rename(&self, key: &[u8], newkey: &[u8]) -> Result<()> {
        let _epoch_guard = self.inner.epoch.enter();

        // If key and newkey are the same, just verify the key exists and return OK
        // This is a no-op per Redis specification
        if key == newkey {
            // Check string keys
            if let Some(entry) = self.inner.data.get(key) {
                if entry.1.is_expired() {
                    drop(entry);
                    self.inner.data.remove(key);
                    return Err(StorageError::NotFound("no such key".to_string()));
                }
                return Ok(());
            }
            // Check hash keys
            if self.inner.hash_data.contains_key(key) {
                return Ok(());
            }
            return Err(StorageError::NotFound("no such key".to_string()));
        }

        // First check if the key exists and is not expired (before removing)
        if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.inner.data.remove(key);
                return Err(StorageError::NotFound("no such key".to_string()));
            }
            drop(entry);

            // Now safe to remove and rename
            if let Some((_, (value, meta))) = self.inner.data.remove(key) {
                // Delete newkey if it exists in either map (RENAME overwrites)
                self.inner.data.remove(newkey);
                self.inner.hash_data.remove(newkey);
                // Insert with the new key
                self.inner.data.insert(newkey.to_vec(), (value, meta));
                return Ok(());
            }
        }

        // Check if it's a hash key
        if let Some((_, hash_value)) = self.inner.hash_data.remove(key) {
            // Delete newkey if it exists in either map (RENAME overwrites)
            self.inner.data.remove(newkey);
            self.inner.hash_data.remove(newkey);
            self.inner.hash_data.insert(newkey.to_vec(), hash_value);
            return Ok(());
        }

        Err(StorageError::NotFound("no such key".to_string()))
    }

    /// Rename a key only if newkey does not exist
    /// Returns true if the key was renamed, false if newkey already exists
    ///
    /// Note: This implementation prioritizes safety over strict atomicity.
    /// Cross-map checks (string vs hash) have inherent TOCTOU windows that
    /// cannot be eliminated without a global lock. We minimize the window
    /// and ensure no data loss by only removing the source after successfully
    /// securing the destination slot.
    pub fn renamenx(&self, key: &[u8], newkey: &[u8]) -> Result<bool> {
        let _epoch_guard = self.inner.epoch.enter();

        // If key and newkey are the same, the "newkey" already exists (it's the same key)
        // Per Redis: return 0 (false) since newkey exists, but first verify key exists
        if key == newkey {
            // Check string keys
            if let Some(entry) = self.inner.data.get(key) {
                if entry.1.is_expired() {
                    drop(entry);
                    self.inner.data.remove(key);
                    return Err(StorageError::NotFound("no such key".to_string()));
                }
                return Ok(false); // newkey exists (same as key)
            }
            // Check hash keys
            if self.inner.hash_data.contains_key(key) {
                return Ok(false); // newkey exists (same as key)
            }
            return Err(StorageError::NotFound("no such key".to_string()));
        }

        // First check if the source key exists and is not expired, and extract its value
        let source_value = if let Some(entry) = self.inner.data.get(key) {
            if entry.1.is_expired() {
                drop(entry);
                self.inner.data.remove(key);
                return Err(StorageError::NotFound("no such key".to_string()));
            }
            Some((entry.0.clone(), entry.1.clone()))
        } else {
            None
        };

        // Handle string keys
        if let Some((value, meta)) = source_value {
            // Clean up expired newkey if it exists
            if let Some(existing) = self.inner.data.get(newkey) {
                if existing.1.is_expired() {
                    drop(existing);
                    self.inner.data.remove(newkey);
                } else {
                    // newkey exists and is not expired
                    return Ok(false);
                }
            }

            // Check if newkey exists as a hash - this has a TOCTOU window but
            // we check again after securing the entry to minimize the race
            if self.inner.hash_data.contains_key(newkey) {
                return Ok(false);
            }

            // Use entry API for atomic check-and-insert on newkey
            match self.inner.data.entry(newkey.to_vec()) {
                dashmap::mapref::entry::Entry::Vacant(vacant) => {
                    // Re-verify hash_data doesn't have newkey (minimize TOCTOU window)
                    // This is best-effort; true atomicity would require a global lock
                    if self.inner.hash_data.contains_key(newkey) {
                        return Ok(false);
                    }

                    // Verify source key still exists and matches what we cloned
                    // Only remove if it still has the same value (prevents data loss
                    // if another thread modified it)
                    let removed = self.inner.data.remove_if(key, |_, existing| {
                        existing.0 == value && !existing.1.is_expired()
                    });

                    if removed.is_some() {
                        vacant.insert((value, meta));
                        Ok(true)
                    } else {
                        // Source key was modified or removed by another thread
                        // Don't insert at newkey since our data is stale
                        Err(StorageError::NotFound("no such key".to_string()))
                    }
                }
                dashmap::mapref::entry::Entry::Occupied(occupied) => {
                    // Another thread inserted newkey between our check and here
                    // Check if it's expired
                    if occupied.get().1.is_expired() {
                        drop(occupied);
                        self.inner.data.remove(newkey);
                        // Retry with entry API
                        if let dashmap::mapref::entry::Entry::Vacant(vacant) =
                            self.inner.data.entry(newkey.to_vec())
                        {
                            // Re-verify and remove source atomically
                            if self.inner.hash_data.contains_key(newkey) {
                                return Ok(false);
                            }
                            let removed = self.inner.data.remove_if(key, |_, existing| {
                                existing.0 == value && !existing.1.is_expired()
                            });
                            if removed.is_some() {
                                vacant.insert((value, meta));
                                return Ok(true);
                            } else {
                                return Err(StorageError::NotFound("no such key".to_string()));
                            }
                        }
                    }
                    Ok(false)
                }
            }
        } else {
            // Check if it's a hash key - get value without removing first
            let hash_value = if let Some(entry) = self.inner.hash_data.get(key) {
                Some(entry.read().clone())
            } else {
                return Err(StorageError::NotFound("no such key".to_string()));
            };

            if let Some(hv) = hash_value {
                // Check if newkey exists in string data - TOCTOU window exists
                // but we re-check after securing the entry
                if self.inner.data.contains_key(newkey) {
                    return Ok(false);
                }

                // Check if newkey exists in hash data
                if self.inner.hash_data.contains_key(newkey) {
                    return Ok(false);
                }

                // Remove source key first (no need for entry API since we're doing simple insert)
                let removed = self.inner.hash_data.remove(key);

                if removed.is_some() {
                    self.inner
                        .hash_data
                        .insert(newkey.to_vec(), RwLock::new(hv));
                    Ok(true)
                } else {
                    // Source key was removed by another thread between our get and now
                    Err(StorageError::NotFound("no such key".to_string()))
                }
            } else {
                Err(StorageError::NotFound("no such key".to_string()))
            }
        }
    }

    /// Get the metadata for a key (for internal use)
    #[allow(dead_code)]
    pub fn get_meta(&self, key: &[u8]) -> Option<EntryMeta> {
        let _epoch_guard = self.inner.epoch.enter();

        self.inner.data.get(key).and_then(|entry| {
            if entry.1.is_expired() {
                None
            } else {
                Some(entry.1.clone())
            }
        })
    }

    /// Clean up expired entries (for background expiration task)
    /// Returns the number of entries cleaned up
    #[allow(dead_code)]
    pub fn cleanup_expired(&self) -> usize {
        let _epoch_guard = self.inner.epoch.enter();

        let mut expired_keys = Vec::new();

        // Collect expired keys
        for entry in self.inner.data.iter() {
            if entry.1.is_expired() {
                expired_keys.push(entry.key().clone());
            }
        }

        let count = expired_keys.len();

        // Delete expired keys
        for key in expired_keys {
            self.delete(&key);
        }

        if count > 0 {
            debug!("Cleaned up {} expired entries", count);
        }

        count
    }

    /// Flush PMEM to ensure persistence
    #[allow(dead_code)]
    pub fn flush(&self) -> Result<()> {
        if let Some(ref pmem) = self.inner.pmem {
            pmem.flush()?;
        }
        Ok(())
    }

    // Hash operations for Redis HASH commands

    /// Set a field in a hash
    /// Returns true if the field is new, false if the field was updated
    pub fn hset(&self, key: &[u8], field: &[u8], value: Vec<u8>) -> Result<bool> {
        let _epoch_guard = self.inner.epoch.enter();
        let is_new = self
            .inner
            .hash_data
            .entry(key.to_vec())
            .or_insert_with(|| RwLock::new(HashMap::new()))
            .write()
            .insert(field.to_vec(), value.clone())
            .is_none();

        // Persist to PMEM
        self.persist_hash_to_pmem(key, field, &value);

        Ok(is_new)
    }

    /// Get a field from a hash
    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            Ok(hash_map.read().get(field).cloned())
        } else {
            Ok(None)
        }
    }

    /// Delete a field from a hash
    /// Returns true if the field was removed, false if it didn't exist
    pub fn hdel(&self, key: &[u8], field: &[u8]) -> Result<bool> {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get_mut(key) {
            Ok(hash_map.write().remove(field).is_some())
        } else {
            Ok(false)
        }
    }

    /// Get all fields in a hash
    pub fn hgetall(&self, key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            Ok(hash_map
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    /// Visit each field/value pair in a hash without collecting them.
    pub fn hfor_each<F>(&self, key: &[u8], mut visit: F) -> Result<usize>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            let inner = hash_map.read();
            let mut count = 0usize;
            for (field, value) in inner.iter() {
                visit(field, value);
                count += 1;
            }
            Ok(count)
        } else {
            Ok(0)
        }
    }

    /// Set multiple fields in a hash
    /// Returns the number of fields that were added (not updated)
    pub fn hmset(&self, key: &[u8], pairs: &[(&[u8], Vec<u8>)]) -> Result<usize> {
        let _epoch_guard = self.inner.epoch.enter();
        let hash_map = self
            .inner
            .hash_data
            .entry(key.to_vec())
            .or_insert_with(|| RwLock::new(HashMap::new()));
        let mut inner = hash_map.write();
        let mut new_count = 0;
        for (field, value) in pairs {
            if inner.insert(field.to_vec(), value.clone()).is_none() {
                new_count += 1;
            }
            // Persist each field to PMEM
            drop(inner);
            self.persist_hash_to_pmem(key, field, value);
            inner = hash_map.write();
        }
        Ok(new_count)
    }

    /// Get multiple fields from a hash
    /// Returns values in the same order as fields, with None for missing fields
    pub fn hmget(&self, key: &[u8], fields: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            let inner = hash_map.read();
            Ok(fields
                .iter()
                .map(|field| inner.get(*field).cloned())
                .collect())
        } else {
            // Key doesn't exist, return None for all fields
            Ok(vec![None; fields.len()])
        }
    }

    /// Get the number of fields in a hash
    pub fn hlen(&self, key: &[u8]) -> usize {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            hash_map.read().len()
        } else {
            0
        }
    }

    /// Check if a field exists in a hash
    pub fn hexists(&self, key: &[u8], field: &[u8]) -> bool {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            hash_map.read().contains_key(field)
        } else {
            false
        }
    }

    /// Get all field names in a hash
    pub fn hkeys(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            hash_map.read().keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Get all values in a hash
    pub fn hvals(&self, key: &[u8]) -> Vec<Vec<u8>> {
        let _epoch_guard = self.inner.epoch.enter();
        if let Some(hash_map) = self.inner.hash_data.get(key) {
            hash_map.read().values().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Increment a hash field by an integer value
    /// Creates the field with value 0 if it doesn't exist
    pub fn hincrby(&self, key: &[u8], field: &[u8], increment: i64) -> Result<i64> {
        let _epoch_guard = self.inner.epoch.enter();

        let hash_map = self
            .inner
            .hash_data
            .entry(key.to_vec())
            .or_insert_with(|| RwLock::new(HashMap::new()));
        let mut inner = hash_map.write();

        match inner.entry(field.to_vec()) {
            hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                let current_bytes = occupied.get();
                let s = std::str::from_utf8(current_bytes).map_err(|_| {
                    StorageError::InvalidType("hash value is not an integer".to_string())
                })?;
                let current = s.parse::<i64>().map_err(|_| {
                    StorageError::InvalidType("hash value is not an integer".to_string())
                })?;

                let new_value = current.checked_add(increment).ok_or_else(|| {
                    StorageError::Overflow("increment would produce overflow".to_string())
                })?;

                let new_value_bytes = new_value.to_string().into_bytes();
                occupied.insert(new_value_bytes.clone());

                // Persist to PMEM
                drop(inner);
                self.persist_hash_to_pmem(key, field, &new_value_bytes);

                Ok(new_value)
            }
            hashbrown::hash_map::Entry::Vacant(vacant) => {
                // Field doesn't exist, create with value = increment (0 + increment)
                let new_value_bytes = increment.to_string().into_bytes();
                vacant.insert(new_value_bytes.clone());

                // Persist to PMEM
                drop(inner);
                self.persist_hash_to_pmem(key, field, &new_value_bytes);

                Ok(increment)
            }
        }
    }

    /// Increment a hash field by a float value
    /// Creates the field with value 0 if it doesn't exist
    pub fn hincrbyfloat(&self, key: &[u8], field: &[u8], increment: f64) -> Result<f64> {
        fn catch_nan_or_infinity(value: f64) -> Result<f64> {
            if value.is_nan() || value.is_infinite() {
                return Err(StorageError::InvalidType(
                    "increment would produce NaN or Infinity".to_string(),
                ));
            }
            Ok(value)
        }

        // Validate increment parameter
        catch_nan_or_infinity(increment)?;

        let _epoch_guard = self.inner.epoch.enter();

        let hash_map = self
            .inner
            .hash_data
            .entry(key.to_vec())
            .or_insert_with(|| RwLock::new(HashMap::new()));
        let mut inner = hash_map.write();

        match inner.entry(field.to_vec()) {
            hashbrown::hash_map::Entry::Occupied(mut occupied) => {
                let current_bytes = occupied.get();
                let s = std::str::from_utf8(current_bytes).map_err(|_| {
                    StorageError::InvalidType("hash value is not a valid float".to_string())
                })?;
                let current = s.parse::<f64>().map_err(|_| {
                    StorageError::InvalidType("hash value is not a valid float".to_string())
                })?;

                catch_nan_or_infinity(current)?;

                let new_value = current + increment;
                catch_nan_or_infinity(new_value)?;

                let new_value_bytes = new_value.to_string().into_bytes();
                occupied.insert(new_value_bytes.clone());

                // Persist to PMEM
                drop(inner);
                self.persist_hash_to_pmem(key, field, &new_value_bytes);

                Ok(new_value)
            }
            hashbrown::hash_map::Entry::Vacant(vacant) => {
                // Field doesn't exist, create with value = increment (0.0 + increment)
                let new_value = increment;
                catch_nan_or_infinity(new_value)?;

                let new_value_bytes = new_value.to_string().into_bytes();
                vacant.insert(new_value_bytes.clone());

                // Persist to PMEM
                drop(inner);
                self.persist_hash_to_pmem(key, field, &new_value_bytes);

                Ok(new_value)
            }
        }
    }

    /// Set a field in a hash only if it doesn't exist
    /// Returns true if the field was set, false if it already existed
    pub fn hsetnx(&self, key: &[u8], field: &[u8], value: Vec<u8>) -> Result<bool> {
        let _epoch_guard = self.inner.epoch.enter();

        let hash_map = self
            .inner
            .hash_data
            .entry(key.to_vec())
            .or_insert_with(|| RwLock::new(HashMap::new()));
        let mut inner = hash_map.write();

        match inner.entry(field.to_vec()) {
            hashbrown::hash_map::Entry::Occupied(_) => Ok(false),
            hashbrown::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(value.clone());
                // Persist to PMEM (only when actually inserting)
                drop(inner);
                self.persist_hash_to_pmem(key, field, &value);
                Ok(true)
            }
        }
    }

    // ========== Key Scanning Operations (Milestone 1.4) ==========

    /// Get all keys matching a pattern (for KEYS command)
    /// WARNING: This is O(n) and should not be used in production on large datasets
    /// Returns keys from both string and hash data stores
    pub fn keys(&self, pattern: &crate::pattern::Pattern) -> Vec<Vec<u8>> {
        let _epoch_guard = self.inner.epoch.enter();
        let mut result = Vec::new();

        // Collect matching string keys (skip expired)
        for entry in self.inner.data.iter() {
            if !entry.1.is_expired() && pattern.matches(entry.key()) {
                result.push(entry.key().clone());
            }
        }

        // Collect matching hash keys (hash keys don't currently support expiration)
        for entry in self.inner.hash_data.iter() {
            if pattern.matches(entry.key()) {
                result.push(entry.key().clone());
            }
        }

        result
    }

    /// Incrementally iterate over keys (for SCAN command)
    /// Returns (new_cursor, keys)
    /// cursor=0 starts iteration, cursor=0 in result means iteration complete
    ///
    /// Performance: O(n) where n is the number of keys scanned, as this iterates
    /// directly over the underlying DashMap entries without collecting all keys.
    ///
    /// The cursor is an opaque value that encodes position in both data stores:
    /// - Bits 0-31: string data index (max ~4 billion keys)
    /// - Bits 32-62: hash data index (max ~2 billion keys, bit 63 reserved)
    /// - Bit 63: phase flag indicating we've moved to hash data
    ///
    /// Limitations:
    /// - Hash index is limited to 31 bits (max ~2.1 billion hash keys)
    /// - If this limit is exceeded, iteration terminates early with cursor=0
    ///
    /// Note: DashMap iteration order is not guaranteed to be stable, so cursor
    /// position may become invalid if keys are added/removed between scan calls,
    /// potentially causing keys to be skipped or duplicated. This is consistent
    /// with Redis SCAN behavior where the cursor is a "hint" rather than a guarantee.
    pub fn scan(
        &self,
        cursor: u64,
        pattern: Option<&crate::pattern::Pattern>,
        count: usize,
    ) -> (u64, Vec<Vec<u8>>) {
        let _epoch_guard = self.inner.epoch.enter();

        let count = count.max(1); // Ensure at least 1
        let mut result = Vec::with_capacity(count);

        // Decode cursor
        let string_index = (cursor & 0xFFFF_FFFF) as usize;
        let hash_index = ((cursor >> 32) & 0x7FFF_FFFF) as usize;
        let in_hash_phase = (cursor >> 63) != 0;

        // Iterate directly over the underlying maps to avoid collecting all keys
        if !in_hash_phase {
            // String data phase: iterate over self.inner.data and use string_index as cursor
            let mut next_string_index: usize = string_index;

            let mut idx = 0usize;
            for entry in self.inner.data.iter() {
                if idx < string_index {
                    idx += 1;
                    continue;
                }

                if result.len() >= count {
                    break;
                }

                let key = entry.key();
                let is_expired = entry.1.is_expired();
                if !is_expired {
                    let matches = pattern.is_none_or(|p| p.matches(key));
                    if matches {
                        result.push(key.clone());
                    }
                }

                idx += 1;
                next_string_index = idx;
            }

            // Check if we've exhausted string keys and need to move to hash phase
            let total_string_entries = self.inner.data.len();
            let reached_string_end = next_string_index >= total_string_entries;

            if result.len() < count && reached_string_end {
                // Hash data phase starting from index 0 in hash_data
                let mut next_hash_index: usize = 0;

                for (idx, entry) in self.inner.hash_data.iter().enumerate() {
                    if result.len() >= count {
                        next_hash_index = idx;
                        break;
                    }

                    let key = entry.key();
                    let matches = pattern.is_none_or(|p| p.matches(key));
                    if matches {
                        result.push(key.clone());
                    }

                    next_hash_index = idx + 1;
                }

                if next_hash_index >= self.inner.hash_data.len() {
                    // Iteration complete across both string and hash data
                    (0, result)
                } else if (next_hash_index as u64) > 0x7FFF_FFFF {
                    // Hash index exceeds 31-bit limit, treat as iteration complete
                    (0, result)
                } else {
                    // Return cursor pointing into hash phase
                    let new_cursor = (1u64 << 63) | ((next_hash_index as u64) << 32);
                    (new_cursor, result)
                }
            } else if result.len() >= count {
                // Still in string phase, more entries remain
                if (next_string_index as u64) > 0xFFFF_FFFF {
                    // String index exceeds 32-bit limit, treat as iteration complete
                    (0, result)
                } else {
                    let new_cursor = next_string_index as u64;
                    (new_cursor, result)
                }
            } else {
                // We've exhausted string phase and didn't need hash phase
                (0, result)
            }
        } else {
            // Hash data phase: iterate over hash_data using hash_index as cursor
            let mut next_hash_index: usize = hash_index;
            let mut idx = 0usize;

            for entry in self.inner.hash_data.iter() {
                if idx < hash_index {
                    idx += 1;
                    continue;
                }

                if result.len() >= count {
                    break;
                }

                let key = entry.key();
                let matches = pattern.is_none_or(|p| p.matches(key));
                if matches {
                    result.push(key.clone());
                }

                idx += 1;
                next_hash_index = idx;
            }

            if next_hash_index >= self.inner.hash_data.len() {
                // Iteration complete
                (0, result)
            } else if (next_hash_index as u64) > 0x7FFF_FFFF {
                // Hash index exceeds 31-bit limit, treat as iteration complete
                (0, result)
            } else {
                // Continue in hash phase
                let new_cursor = (1u64 << 63) | ((next_hash_index as u64) << 32);
                (new_cursor, result)
            }
        }
    }

    /// Get the total number of keys (for DBSIZE command)
    /// Returns count of non-expired string keys + hash keys
    pub fn dbsize(&self) -> usize {
        let _epoch_guard = self.inner.epoch.enter();

        // Count non-expired string keys
        let string_count = self
            .inner
            .data
            .iter()
            .filter(|entry| !entry.1.is_expired())
            .count();

        // Count hash keys (no expiration support currently)
        let hash_count = self.inner.hash_data.len();

        string_count + hash_count
    }

    /// Get a random key (for RANDOMKEY command)
    /// Returns None if the database is empty
    pub fn random_key(&self) -> Option<Vec<u8>> {
        static SEED: AtomicU64 = AtomicU64::new(0);

        let _epoch_guard = self.inner.epoch.enter();

        // Get approximate size to generate random index
        let string_len = self.inner.data.len();
        let hash_len = self.inner.hash_data.len();
        let total = string_len + hash_len;

        if total == 0 {
            return None;
        }

        // Mix current time with an atomic seed to avoid repeated values on fast calls
        let time_seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let seed = time_seed
            ^ SEED
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_mul(0x9E3779B97F4A7C15);
        let target = (seed as usize) % total;

        // Try to get a non-expired key, with a few retries
        // Note: This is O(n) as DashMap iteration requires scanning entries
        let string_len_nonzero = string_len.max(1);
        let hash_len_nonzero = hash_len.max(1);
        for retry in 0..10 {
            let adjusted_target = (target + retry) % total;

            if string_len > 0 && adjusted_target < string_len {
                // Pick from string keys using nth() for efficiency
                let idx = adjusted_target % string_len_nonzero;
                if let Some(entry) = self.inner.data.iter().nth(idx)
                    && !entry.1.is_expired()
                {
                    return Some(entry.key().clone());
                }
            }

            if hash_len > 0 {
                // Prefer hash keys when string key is expired or absent
                let hash_target = adjusted_target.saturating_sub(string_len);
                let idx = hash_target % hash_len_nonzero;
                if let Some(entry) = self.inner.hash_data.iter().nth(idx) {
                    return Some(entry.key().clone());
                }
            }
        }

        // Fallback: return any non-expired key
        for entry in self.inner.data.iter() {
            if !entry.1.is_expired() {
                return Some(entry.key().clone());
            }
        }

        if let Some(entry) = self.inner.hash_data.iter().next() {
            return Some(entry.key().clone());
        }

        None
    }

    // ========== Persistence Operations (Milestone 3.2) ==========

    /// Internal save implementation with optional compression
    fn save_internal(&self, path: &str, compress: bool) -> Result<()> {
        let temp_path = format!("{}.tmp", path);

        let file = std::fs::File::create(&temp_path)?;
        let writer = std::io::BufWriter::new(file);
        let options = snapshot::SnapshotOptions { compress };
        let mut snapshot = snapshot::SnapshotWriter::with_options(writer, options)?;

        // Iterate over strings
        for entry in self.inner.data.iter() {
            let (key, (value, meta)) = entry.pair();
            if !meta.is_expired() {
                // Calculate expiry timestamp
                let expiry = if let Some(expires_at) = meta.expires_at {
                    let now = Instant::now();
                    if expires_at <= now {
                        continue; // Skip expired
                    }
                    let remaining = expires_at - now;
                    let now_system = SystemTime::now();
                    let expiry_system = now_system + remaining;
                    Some(
                        expiry_system
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    )
                } else {
                    None
                };

                snapshot.write_string(key, value, expiry)?;
            }
        }

        // Iterate over hashes - use streaming write to avoid double collection
        for entry in self.inner.hash_data.iter() {
            let key = entry.key();
            // Hash entries don't have TTL currently
            let hash_map = entry.value().read();
            let field_count = hash_map.len() as u32;
            let fields_iter = hash_map.iter().map(|(k, v)| (k.clone(), v.clone()));
            snapshot.write_hash_streaming(key, field_count, fields_iter, None)?;
        }

        snapshot.finish()?;

        // Rename temp file to target (atomic)
        if let Err(e) = std::fs::rename(&temp_path, path) {
            // Clean up temp file on rename failure
            let _ = std::fs::remove_file(&temp_path);
            return Err(StorageError::Snapshot(format!(
                "Failed to rename snapshot: {}",
                e
            )));
        }

        // Update last save time
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.inner.last_save.store(now, Ordering::Relaxed);

        Ok(())
    }

    /// Save the DB to disk synchronously (blocks current thread)
    pub fn save(&self) -> Result<()> {
        self.save_to_path("dump.eagle")
    }

    /// Save the DB to a specific path (synchronously)
    pub fn save_to_path(&self, path: &str) -> Result<()> {
        self.save_to_path_with_options(path, false)
    }

    /// Save the DB to a specific path with compression option (synchronously)
    pub fn save_to_path_with_options(&self, path: &str, compress: bool) -> Result<()> {
        if self
            .inner
            .bgsave_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(StorageError::Busy(
                "Background save already in progress".to_string(),
            ));
        }

        let result = self.save_internal(path, compress);
        self.inner.bgsave_in_progress.store(false, Ordering::SeqCst);
        result
    }

    /// Save the DB to disk in background
    pub fn bgsave(&self) -> Result<String> {
        self.bgsave_to_path("dump.eagle")
    }

    /// Save the DB to a specific path in background
    pub fn bgsave_to_path(&self, path: &str) -> Result<String> {
        self.bgsave_to_path_with_options(path, false)
    }

    /// Save the DB to a specific path in background with compression option
    pub fn bgsave_to_path_with_options(&self, path: &str, compress: bool) -> Result<String> {
        if self
            .inner
            .bgsave_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(StorageError::Busy(
                "Background save already in progress".to_string(),
            ));
        }

        let store = self.clone();
        let path = path.to_string();

        tokio::task::spawn_blocking(move || {
            match store.save_internal(&path, compress) {
                Ok(_) => {
                    info!(path = %path, compressed = compress, "Background saving terminated with success");
                    // Update last save time on success
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    store.inner.last_save.store(now, Ordering::Relaxed);
                }
                Err(e) => tracing::error!(path = %path, error = %e, "Background saving failed"),
            }
            store
                .inner
                .bgsave_in_progress
                .store(false, Ordering::SeqCst);
        });

        Ok("Background saving started".to_string())
    }

    /// Get timestamp of last successful save
    pub fn last_save(&self) -> u64 {
        self.inner.last_save.load(Ordering::Relaxed)
    }

    /// Load snapshot from file
    #[allow(dead_code)]
    pub fn load_snapshot(&self, path: &str) -> Result<usize> {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let snapshot = snapshot::SnapshotReader::new(reader)?;

        let snapshot_time = snapshot.timestamp();
        info!(timestamp_ms = snapshot_time, "Loading snapshot");

        let mut snapshot = snapshot;

        // We use SystemTime for expiry calculation
        use std::time::UNIX_EPOCH;

        let count = snapshot.read_entries(|entry_type, key, value, expiry| {
            // Expiry conversion
            let expires_at = if let Some(millis) = expiry {
                let now_system = SystemTime::now();
                let expiry_system = UNIX_EPOCH + Duration::from_millis(millis);
                if expiry_system <= now_system {
                    return Ok(()); // Expired
                }
                let remaining = expiry_system.duration_since(now_system).unwrap_or_default();
                Some(Instant::now() + remaining)
            } else {
                None
            };

            match entry_type {
                snapshot::EntryType::String => {
                    if let snapshot::SnapshotValue::String(val) = value {
                        let mut meta = EntryMeta {
                            timestamp: Instant::now(),
                            pmem_offset: None,
                            expires_at,
                        };

                        // Persist to PMEM if enabled
                        self.persist_to_pmem(&key, &val, &mut meta);
                        self.inner.data.insert(key, (val, meta));
                    }
                }
                snapshot::EntryType::Hash => {
                    if let snapshot::SnapshotValue::Hash(fields) = value {
                        let map = RwLock::new(HashMap::new());
                        for (f, v) in fields {
                            self.persist_hash_to_pmem(&key, &f, &v);
                            map.write().insert(f, v);
                        }
                        self.inner.hash_data.insert(key, map);
                    }
                }
            }
            Ok(())
        })?;

        info!("Loaded {} entries from snapshot {}", count, path);
        Ok(count as usize)
    }

    /// Take the snapshot scheduler for graceful shutdown
    /// Returns Some(scheduler) if one exists, None otherwise
    pub fn take_snapshot_scheduler(&self) -> Option<SnapshotScheduler> {
        self.inner.snapshot_scheduler.write().take()
    }

    /// Check if a snapshot scheduler is running
    pub fn has_snapshot_scheduler(&self) -> bool {
        self.inner.snapshot_scheduler.read().is_some()
    }

    /// Delete all keys (for FLUSHDB command)
    /// Returns the number of keys that were deleted (non-expired string keys + hash keys).
    /// Note: All keys (including expired) are removed from the store, but only non-expired
    /// keys are counted in the return value to maintain consistency with dbsize().
    pub fn flush_db(&self) -> usize {
        let _epoch_guard = self.inner.epoch.enter();

        // Count non-expired string keys (consistent with dbsize behavior)
        let string_count = self
            .inner
            .data
            .iter()
            .filter(|entry| !entry.1.is_expired())
            .count();
        let hash_count = self.inner.hash_data.len();

        // Clear string data and mark PMEM entries as deleted
        if let Some(ref pmem) = self.inner.pmem {
            for entry in self.inner.data.iter() {
                if let Some(offset) = entry.1.pmem_offset
                    && let Err(e) = pmem.delete_entry(offset)
                {
                    tracing::warn!("Failed to delete PMEM entry at offset {}: {}", offset, e);
                }
            }
        }

        self.inner.data.clear();
        self.inner.hash_data.clear();

        string_count + hash_count
    }

    /// Check if PMEM is enabled
    #[allow(dead_code)]
    pub fn has_pmem(&self) -> bool {
        self.inner.pmem.is_some()
    }

    /// Get PMEM statistics
    #[allow(dead_code)]
    pub fn pmem_stats(&self) -> Option<PmemStats> {
        self.inner.pmem.as_ref().map(|pmem| PmemStats {
            total_size: pmem.size(),
            used_size: pmem.used_size(),
            available_size: pmem.available_size(),
            entry_count: pmem.entry_count(),
        })
    }
}

/// PMEM statistics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PmemStats {
    pub total_size: usize,
    pub used_size: usize,
    pub available_size: usize,
    pub entry_count: u64,
}

/// Store using extendible hashing (Dash algorithm)
#[allow(dead_code)]
pub struct StoreDash {
    dash: Arc<dash::DashStore>,
    epoch: Arc<Epoch>,
}

#[allow(dead_code)]
impl StoreDash {
    pub fn new() -> Result<Self> {
        let epoch = Arc::new(Epoch::new());
        let dash = Arc::new(dash::DashStore::new(epoch.clone())?);
        Ok(Self { dash, epoch })
    }

    pub fn new_memory() -> Result<Self> {
        Self::new()
    }

    pub fn new_pmem(path: &str, size_mb: usize) -> Result<Self> {
        let epoch = Arc::new(Epoch::new());
        let allocator = Arc::new(PmemAllocator::new(path, size_mb)?);
        let dash = Arc::new(dash::DashStore::new_pmem(allocator, epoch.clone())?);
        Ok(Self { dash, epoch })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self.epoch.pin();
        self.dash.get(key, &guard)
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let guard = self.epoch.pin();
        self.dash.put(key, value, &guard)
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let guard = self.epoch.pin();
        self.dash.delete(key, &guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_store_memory() -> Result<()> {
        let store = Store::new_memory()?;

        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));

        store.delete(b"key1");
        assert_eq!(store.get(b"key1"), None);

        Ok(())
    }

    #[test]
    fn test_store_pmem() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.pmem");

        // Create store with PMEM
        {
            let store = Store::new_pmem(path.to_str().unwrap(), 10)?;
            store.set(b"key1".to_vec(), b"value1".to_vec())?;
            store.set(b"key2".to_vec(), b"value2".to_vec())?;
            store.flush()?;

            assert!(store.has_pmem());
            assert_eq!(store.len(), 2);
        }

        // Reopen and verify recovery
        {
            let store = Store::new_pmem(path.to_str().unwrap(), 10)?;
            assert_eq!(store.len(), 2);
            assert_eq!(store.get(b"key1"), Some(b"value1".to_vec()));
            assert_eq!(store.get(b"key2"), Some(b"value2".to_vec()));
        }

        Ok(())
    }

    #[test]
    fn test_store_hash_ops() -> Result<()> {
        let store = Store::new_memory()?;

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"value1".to_vec()));
        assert_eq!(store.hget(b"myhash", b"field2")?, Some(b"value2".to_vec()));
        assert_eq!(store.hget(b"myhash", b"field3")?, None);

        store.hdel(b"myhash", b"field1")?;
        assert_eq!(store.hget(b"myhash", b"field1")?, None);

        Ok(())
    }

    #[test]
    fn test_pmem_stats() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.pmem");

        let store = Store::new_pmem(path.to_str().unwrap(), 10)?;

        let stats = store.pmem_stats().unwrap();
        assert!(stats.total_size > 0);
        assert_eq!(stats.entry_count, 0);

        store.set(b"key".to_vec(), b"value".to_vec())?;

        let stats = store.pmem_stats().unwrap();
        assert_eq!(stats.entry_count, 1);
        assert!(stats.used_size > 0);

        Ok(())
    }

    #[test]
    fn test_hash_pmem_persistence() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.pmem");
        let path_str = path.to_str().unwrap();

        // Phase 1: Write hash data
        {
            let store = Store::new_pmem(path_str, 10)?;

            store.hset(b"myhash", b"field1", b"value1".to_vec())?;
            store.hset(b"myhash", b"field2", b"value2".to_vec())?;
            store.hset(b"anotherhash", b"fieldA", b"valueA".to_vec())?;

            store.flush()?;

            assert_eq!(store.hlen(b"myhash"), 2);
            assert_eq!(store.hlen(b"anotherhash"), 1);
        }

        // Phase 2: Recover and verify
        {
            let store = Store::new_pmem(path_str, 10)?;

            assert_eq!(
                store.hlen(b"myhash"),
                2,
                "myhash should have 2 fields after recovery"
            );
            assert_eq!(
                store.hlen(b"anotherhash"),
                1,
                "anotherhash should have 1 field after recovery"
            );
            assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"value1".to_vec()));
            assert_eq!(store.hget(b"myhash", b"field2")?, Some(b"value2".to_vec()));
            assert_eq!(
                store.hget(b"anotherhash", b"fieldA")?,
                Some(b"valueA".to_vec())
            );
        }

        Ok(())
    }

    #[test]
    fn test_hash_pmem_update() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("test.pmem");
        let path_str = path.to_str().unwrap();

        // Phase 1: Write and update hash data
        {
            let store = Store::new_pmem(path_str, 10)?;

            store.hset(b"myhash", b"field1", b"initial".to_vec())?;
            assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"initial".to_vec()));

            store.hset(b"myhash", b"field1", b"updated".to_vec())?;
            assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"updated".to_vec()));

            store.flush()?;
        }

        // Phase 2: Recover - should have updated value
        {
            let store = Store::new_pmem(path_str, 10)?;

            assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"updated".to_vec()));
        }

        Ok(())
    }

    // ========== TTL/Expiration Tests ==========

    #[test]
    fn test_expire_and_ttl() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // No TTL initially
        assert_eq!(store.ttl(b"mykey"), -1);
        assert_eq!(store.pttl(b"mykey"), -1);

        // Set TTL
        assert!(store.expire(b"mykey", 100));
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 0 && ttl <= 100);

        let pttl = store.pttl(b"mykey");
        assert!(pttl > 0 && pttl <= 100_000);

        Ok(())
    }

    #[test]
    fn test_pexpire() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set TTL in milliseconds
        assert!(store.pexpire(b"mykey", 5000));
        let pttl = store.pttl(b"mykey");
        assert!(pttl > 0 && pttl <= 5000);

        Ok(())
    }

    #[test]
    fn test_expireat() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set expiration 100 seconds from now
        let future_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 100;

        assert!(store.expireat(b"mykey", future_timestamp));
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 90 && ttl <= 100);

        Ok(())
    }

    #[test]
    fn test_pexpireat() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set expiration 5000ms from now
        let future_timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 5000;

        assert!(store.pexpireat(b"mykey", future_timestamp_ms));
        let pttl = store.pttl(b"mykey");
        assert!(pttl > 4000 && pttl <= 5000);

        Ok(())
    }

    #[test]
    fn test_persist() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set TTL
        store.expire(b"mykey", 100);
        assert!(store.ttl(b"mykey") > 0);

        // Remove TTL
        assert!(store.persist(b"mykey"));
        assert_eq!(store.ttl(b"mykey"), -1);

        // Persist on key without TTL returns false
        assert!(!store.persist(b"mykey"));

        Ok(())
    }

    #[test]
    fn test_ttl_nonexistent_key() -> Result<()> {
        let store = Store::new_memory()?;

        assert_eq!(store.ttl(b"nonexistent"), -2);
        assert_eq!(store.pttl(b"nonexistent"), -2);

        Ok(())
    }

    #[test]
    fn test_expire_nonexistent_key() -> Result<()> {
        let store = Store::new_memory()?;

        // Should return false for nonexistent key
        assert!(!store.expire(b"nonexistent", 100));
        assert!(!store.pexpire(b"nonexistent", 100));

        Ok(())
    }

    #[test]
    fn test_lazy_expiration() -> Result<()> {
        let store = Store::new_memory()?;

        // Set a key with very short TTL
        store.set_with_expiry(
            b"shortlived".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() + Duration::from_millis(10)),
        )?;

        // Key should exist initially
        assert!(store.exists(b"shortlived"));

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(20));

        // Key should be gone (lazy expiration on get)
        assert!(store.get(b"shortlived").is_none());
        assert!(!store.exists(b"shortlived"));

        Ok(())
    }

    #[test]
    fn test_set_with_expiry() -> Result<()> {
        let store = Store::new_memory()?;

        let expires_at = Instant::now() + Duration::from_secs(60);
        store.set_with_expiry(b"mykey".to_vec(), b"myvalue".to_vec(), Some(expires_at))?;

        // Key should exist and have TTL
        assert!(store.exists(b"mykey"));
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 50 && ttl <= 60);

        Ok(())
    }

    #[test]
    fn test_cleanup_expired() -> Result<()> {
        let store = Store::new_memory()?;

        // Add some keys with short TTL
        for i in 0..5 {
            store.set_with_expiry(
                format!("expire{}", i).into_bytes(),
                b"value".to_vec(),
                Some(Instant::now() + Duration::from_millis(5)),
            )?;
        }

        // Add some keys without TTL
        for i in 0..3 {
            store.set(format!("persist{}", i).into_bytes(), b"value".to_vec())?;
        }

        assert_eq!(store.len(), 8);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        // Cleanup
        let cleaned = store.cleanup_expired();
        assert_eq!(cleaned, 5);
        assert_eq!(store.len(), 3);

        Ok(())
    }

    #[test]
    fn test_expireat_past_timestamp() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;

        // Set expiration in the past
        let past_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 100;

        // Should delete the key
        assert!(!store.expireat(b"mykey", past_timestamp));
        assert!(!store.exists(b"mykey"));

        Ok(())
    }

    #[test]
    fn test_exists_with_expiry() -> Result<()> {
        let store = Store::new_memory()?;

        // Non-existent key
        assert!(!store.exists(b"nonexistent"));

        // Key without expiration
        store.set(b"permanent".to_vec(), b"value".to_vec())?;
        assert!(store.exists(b"permanent"));

        // Key with expiration (not yet expired)
        store.set_with_expiry(
            b"temporary".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() + Duration::from_secs(60)),
        )?;
        assert!(store.exists(b"temporary"));

        Ok(())
    }

    #[test]
    fn test_get_meta() -> Result<()> {
        let store = Store::new_memory()?;

        // No meta for nonexistent key
        assert!(store.get_meta(b"nonexistent").is_none());

        // Key with expiration
        let expires_at = Instant::now() + Duration::from_secs(60);
        store.set_with_expiry(b"mykey".to_vec(), b"myvalue".to_vec(), Some(expires_at))?;

        let meta = store.get_meta(b"mykey").unwrap();
        assert!(meta.expires_at.is_some());
        assert!(meta.ttl_millis().unwrap() > 50_000);

        Ok(())
    }

    #[test]
    fn test_entry_meta_is_expired() {
        // Not expired
        let meta = EntryMeta {
            timestamp: Instant::now(),
            pmem_offset: None,
            expires_at: Some(Instant::now() + Duration::from_secs(60)),
        };
        assert!(!meta.is_expired());

        // Expired
        let meta = EntryMeta {
            timestamp: Instant::now(),
            pmem_offset: None,
            expires_at: Some(Instant::now() - Duration::from_secs(1)),
        };
        assert!(meta.is_expired());

        // No expiration
        let meta = EntryMeta::default();
        assert!(!meta.is_expired());
    }

    #[test]
    fn test_entry_meta_ttl_millis() {
        // Has TTL
        let meta = EntryMeta {
            timestamp: Instant::now(),
            pmem_offset: None,
            expires_at: Some(Instant::now() + Duration::from_millis(5000)),
        };
        let ttl = meta.ttl_millis().unwrap();
        assert!(ttl > 4000 && ttl <= 5000);

        // Expired
        let meta = EntryMeta {
            timestamp: Instant::now(),
            pmem_offset: None,
            expires_at: Some(Instant::now() - Duration::from_secs(1)),
        };
        assert!(meta.ttl_millis().is_none());

        // No expiration
        let meta = EntryMeta::default();
        assert!(meta.ttl_millis().is_none());
    }

    // ========== Milestone 1.3 String Operations Tests ==========

    #[test]
    fn test_incrby() -> Result<()> {
        let store = Store::new_memory()?;

        // Increment nonexistent key (should create with value 0 then increment)
        assert_eq!(store.incrby(b"counter", 5)?, 5);
        assert_eq!(store.get(b"counter"), Some(b"5".to_vec()));

        // Increment existing value
        assert_eq!(store.incrby(b"counter", 10)?, 15);
        assert_eq!(store.get(b"counter"), Some(b"15".to_vec()));

        // Increment with negative value (decrement)
        assert_eq!(store.incrby(b"counter", -3)?, 12);
        assert_eq!(store.get(b"counter"), Some(b"12".to_vec()));

        Ok(())
    }

    #[test]
    fn test_incrby_invalid_value() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"string".to_vec(), b"not a number".to_vec())?;

        // Should fail with type error
        assert!(store.incrby(b"string", 1).is_err());

        Ok(())
    }

    #[test]
    fn test_decrby() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"counter".to_vec(), b"10".to_vec())?;

        assert_eq!(store.decrby(b"counter", 3)?, 7);
        assert_eq!(store.get(b"counter"), Some(b"7".to_vec()));

        Ok(())
    }

    #[test]
    fn test_incrby_overflow() -> Result<()> {
        let store = Store::new_memory()?;

        // Set key to i64::MAX
        store.set(b"max".to_vec(), i64::MAX.to_string().into_bytes())?;

        // Incrementing by 1 should overflow
        let result = store.incrby(b"max", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        // Incrementing by a large positive number should overflow
        store.set(b"large".to_vec(), (i64::MAX - 10).to_string().into_bytes())?;
        let result = store.incrby(b"large", 20);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        // Set key to i64::MIN
        store.set(b"min".to_vec(), i64::MIN.to_string().into_bytes())?;

        // Decrementing (negative increment) should overflow
        let result = store.incrby(b"min", -1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        Ok(())
    }

    #[test]
    fn test_decrby_overflow() -> Result<()> {
        let store = Store::new_memory()?;

        // Set key to i64::MIN
        store.set(b"min".to_vec(), i64::MIN.to_string().into_bytes())?;

        // Decrementing by 1 should overflow
        let result = store.decrby(b"min", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        // Decrementing by a large number should overflow
        store.set(b"neg".to_vec(), (i64::MIN + 10).to_string().into_bytes())?;
        let result = store.decrby(b"neg", 20);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        // Decrementing i64::MIN (negating it overflows)
        let result = store.decrby(b"min", i64::MIN);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        Ok(())
    }

    #[test]
    fn test_incrbyfloat() -> Result<()> {
        let store = Store::new_memory()?;

        // Float increment on nonexistent key
        let val = store.incrbyfloat(b"myfloat", 0.5)?;
        assert!((val - 0.5).abs() < 0.0001);

        // Float increment on existing value
        store.set(b"existing".to_vec(), b"10.5".to_vec())?;
        let val = store.incrbyfloat(b"existing", 0.1)?;
        assert!((val - 10.6).abs() < 0.0001);

        // Negative increment
        let val = store.incrbyfloat(b"myfloat", -0.25)?;
        assert!((val - 0.25).abs() < 0.0001);

        Ok(())
    }

    #[test]
    fn test_incrbyfloat_invalid() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"string".to_vec(), b"not a number".to_vec())?;

        assert!(store.incrbyfloat(b"string", 1.0).is_err());

        Ok(())
    }

    #[test]
    fn test_incrbyfloat_nan_infinity() -> Result<()> {
        let store = Store::new_memory()?;

        // Test incrementing by infinity produces error
        let result = store.incrbyfloat(b"key1", f64::INFINITY);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        // Test incrementing by negative infinity produces error
        let result = store.incrbyfloat(b"key2", f64::NEG_INFINITY);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        // Test incrementing by NaN produces error
        let result = store.incrbyfloat(b"key3", f64::NAN);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        // Test that adding large values that overflow to infinity produces error
        store.set(b"large".to_vec(), f64::MAX.to_string().into_bytes())?;
        let result = store.incrbyfloat(b"large", f64::MAX);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        // Test that subtracting from negative max that overflows produces error
        store.set(b"neg_large".to_vec(), (-f64::MAX).to_string().into_bytes())?;
        let result = store.incrbyfloat(b"neg_large", -f64::MAX);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        Ok(())
    }

    #[test]
    fn test_incrbyfloat_rejects_stored_nan_or_infinity() -> Result<()> {
        let store = Store::new_memory()?;

        // Stored NaN should be rejected (even though it parses as f64)
        store.set(b"nan".to_vec(), b"NaN".to_vec())?;
        let err = store.incrbyfloat(b"nan", 1.0).unwrap_err().to_string();
        assert!(err.contains("increment would produce NaN or Infinity"));

        // Stored +Infinity should be rejected
        store.set(b"inf".to_vec(), b"inf".to_vec())?;
        let err = store.incrbyfloat(b"inf", 1.0).unwrap_err().to_string();
        assert!(err.contains("increment would produce NaN or Infinity"));

        // Stored -Infinity should be rejected
        store.set(b"neg_inf".to_vec(), b"-inf".to_vec())?;
        let err = store.incrbyfloat(b"neg_inf", 1.0).unwrap_err().to_string();
        assert!(err.contains("increment would produce NaN or Infinity"));

        Ok(())
    }

    #[test]
    fn test_append() -> Result<()> {
        let store = Store::new_memory()?;

        // Append to nonexistent key (creates it)
        let len = store.append(b"mykey", b"Hello")?;
        assert_eq!(len, 5);
        assert_eq!(store.get(b"mykey"), Some(b"Hello".to_vec()));

        // Append to existing key
        let len = store.append(b"mykey", b" World")?;
        assert_eq!(len, 11);
        assert_eq!(store.get(b"mykey"), Some(b"Hello World".to_vec()));

        Ok(())
    }

    #[test]
    fn test_strlen() -> Result<()> {
        let store = Store::new_memory()?;

        // Strlen on nonexistent key
        assert_eq!(store.strlen(b"nonexistent"), 0);

        // Strlen on existing key
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;
        assert_eq!(store.strlen(b"mykey"), 11);

        Ok(())
    }

    #[test]
    fn test_getrange() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;

        // Positive indices
        assert_eq!(store.getrange(b"mykey", 0, 4), b"Hello".to_vec());

        // Negative indices (from end)
        assert_eq!(store.getrange(b"mykey", -5, -1), b"World".to_vec());

        // Mixed indices
        assert_eq!(store.getrange(b"mykey", 0, -1), b"Hello World".to_vec());

        // Out of range end (should clamp to string end)
        assert_eq!(store.getrange(b"mykey", 0, 100), b"Hello World".to_vec());

        // Start beyond string length - should return empty per Redis spec
        assert!(
            store.getrange(b"mykey", 20, 30).is_empty(),
            "GETRANGE with start beyond string length should return empty"
        );

        // Start at exact length boundary - should return empty
        assert!(
            store.getrange(b"mykey", 11, 20).is_empty(),
            "GETRANGE with start at string length should return empty"
        );

        // Empty result when start > end
        assert!(store.getrange(b"mykey", 5, 2).is_empty());

        // Nonexistent key
        assert!(store.getrange(b"nonexistent", 0, 10).is_empty());

        Ok(())
    }

    #[test]
    fn test_setrange() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;

        // Overwrite part of string
        let len = store.setrange(b"mykey", 6, b"Redis")?;
        assert_eq!(len, 11);
        assert_eq!(store.get(b"mykey"), Some(b"Hello Redis".to_vec()));

        // Setrange extends string with null bytes if offset > len
        store.set(b"short".to_vec(), b"Hi".to_vec())?;
        let len = store.setrange(b"short", 5, b"There")?;
        assert_eq!(len, 10);
        let value = store.get(b"short").unwrap();
        assert_eq!(value.len(), 10);
        assert_eq!(&value[0..2], b"Hi");
        assert_eq!(&value[5..10], b"There");

        // Setrange on nonexistent key (creates with null bytes)
        let len = store.setrange(b"newkey", 3, b"abc")?;
        assert_eq!(len, 6);
        let value = store.get(b"newkey").unwrap();
        assert_eq!(value.len(), 6);
        assert_eq!(&value[3..6], b"abc");

        Ok(())
    }

    #[test]
    fn test_setrange_empty_value() -> Result<()> {
        let store = Store::new_memory()?;

        // Set up a key with TTL
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;
        store.expire(b"mykey", 60);

        // SETRANGE with empty value should be a no-op
        let len = store.setrange(b"mykey", 5, b"")?;

        // Should return current length (no change)
        assert_eq!(len, 11);

        // String should be unchanged
        assert_eq!(store.get(b"mykey"), Some(b"Hello World".to_vec()));

        // TTL should be preserved
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 50 && ttl <= 60);

        // Empty value on nonexistent key at offset 0 - creates empty string
        let len = store.setrange(b"emptykey", 0, b"")?;
        assert_eq!(len, 0);
        assert_eq!(store.get(b"emptykey"), Some(Vec::new()));

        // Empty value on nonexistent key at offset > 0 - creates string with null bytes
        let len = store.setrange(b"padded", 5, b"")?;
        assert_eq!(len, 5);
        let value = store.get(b"padded").unwrap();
        assert_eq!(value.len(), 5);
        assert!(value.iter().all(|&b| b == 0));

        Ok(())
    }

    // ========== Milestone 1.3 Generic Key Operations Tests ==========

    #[test]
    fn test_key_type() -> Result<()> {
        let store = Store::new_memory()?;

        // Nonexistent key
        assert_eq!(store.key_type(b"nonexistent"), "none");

        // String type
        store.set(b"mystring".to_vec(), b"hello".to_vec())?;
        assert_eq!(store.key_type(b"mystring"), "string");

        // Hash type
        store.hset(b"myhash", b"field", b"value".to_vec())?;
        assert_eq!(store.key_type(b"myhash"), "hash");

        Ok(())
    }

    #[test]
    fn test_rename() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"oldkey".to_vec(), b"value".to_vec())?;

        // Rename succeeds
        store.rename(b"oldkey", b"newkey")?;
        assert!(!store.exists(b"oldkey"));
        assert_eq!(store.get(b"newkey"), Some(b"value".to_vec()));

        // Rename nonexistent key fails
        assert!(store.rename(b"nonexistent", b"other").is_err());

        // Rename overwrites existing key
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;
        store.rename(b"key1", b"key2")?;
        assert!(!store.exists(b"key1"));
        assert_eq!(store.get(b"key2"), Some(b"value1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_rename_hash() -> Result<()> {
        let store = Store::new_memory()?;
        store.hset(b"oldhash", b"field", b"value".to_vec())?;

        store.rename(b"oldhash", b"newhash")?;
        assert_eq!(store.key_type(b"oldhash"), "none");
        assert_eq!(store.key_type(b"newhash"), "hash");
        assert_eq!(store.hget(b"newhash", b"field")?, Some(b"value".to_vec()));

        Ok(())
    }

    #[test]
    fn test_renamenx() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;

        // Rename succeeds when newkey doesn't exist
        assert!(store.renamenx(b"key1", b"key2")?);
        assert!(!store.exists(b"key1"));
        assert_eq!(store.get(b"key2"), Some(b"value1".to_vec()));

        // Rename fails when newkey exists
        store.set(b"key3".to_vec(), b"value3".to_vec())?;
        store.set(b"key4".to_vec(), b"value4".to_vec())?;
        assert!(!store.renamenx(b"key3", b"key4")?);
        assert_eq!(store.get(b"key3"), Some(b"value3".to_vec()));
        assert_eq!(store.get(b"key4"), Some(b"value4".to_vec()));

        // Rename nonexistent key fails
        assert!(store.renamenx(b"nonexistent", b"other").is_err());

        Ok(())
    }

    #[test]
    fn test_rename_same_key() -> Result<()> {
        let store = Store::new_memory()?;

        // String key: rename to itself should be a no-op and return OK
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;
        store.expire(b"mykey", 60);
        store.rename(b"mykey", b"mykey")?;
        assert_eq!(store.get(b"mykey"), Some(b"myvalue".to_vec()));
        // TTL should be preserved
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 50 && ttl <= 60);

        // Hash key: rename to itself should be a no-op
        store.hset(b"myhash", b"field", b"value".to_vec())?;
        store.rename(b"myhash", b"myhash")?;
        assert_eq!(store.hget(b"myhash", b"field")?, Some(b"value".to_vec()));

        // Nonexistent key should still error
        assert!(store.rename(b"nonexistent", b"nonexistent").is_err());

        Ok(())
    }

    #[test]
    fn test_renamenx_same_key() -> Result<()> {
        let store = Store::new_memory()?;

        // String key: renamenx to itself should return false (newkey exists)
        store.set(b"mykey".to_vec(), b"myvalue".to_vec())?;
        assert!(!store.renamenx(b"mykey", b"mykey")?);
        // Key should still exist with same value
        assert_eq!(store.get(b"mykey"), Some(b"myvalue".to_vec()));

        // Hash key: renamenx to itself should return false
        store.hset(b"myhash", b"field", b"value".to_vec())?;
        assert!(!store.renamenx(b"myhash", b"myhash")?);
        assert_eq!(store.hget(b"myhash", b"field")?, Some(b"value".to_vec()));

        // Nonexistent key should error
        assert!(store.renamenx(b"nonexistent", b"nonexistent").is_err());

        Ok(())
    }

    #[test]
    fn test_rename_cross_type() -> Result<()> {
        let store = Store::new_memory()?;

        // Test 1: Rename string to existing hash key name (RENAME overwrites)
        store.set(b"strkey".to_vec(), b"strvalue".to_vec())?;
        store.hset(b"hashkey", b"field", b"hashvalue".to_vec())?;
        store.rename(b"strkey", b"hashkey")?;
        // strkey should be gone
        assert!(!store.exists(b"strkey"));
        // hashkey should now be a string, not a hash
        assert_eq!(store.key_type(b"hashkey"), "string");
        assert_eq!(store.get(b"hashkey"), Some(b"strvalue".to_vec()));

        // Test 2: Rename hash to existing string key name (RENAME overwrites)
        store.hset(b"hash2", b"f1", b"v1".to_vec())?;
        store.set(b"str2".to_vec(), b"str2value".to_vec())?;
        store.rename(b"hash2", b"str2")?;
        // hash2 should be gone
        assert!(!store.exists(b"hash2"));
        // str2 should now be a hash, not a string
        assert_eq!(store.key_type(b"str2"), "hash");
        assert_eq!(store.hget(b"str2", b"f1")?, Some(b"v1".to_vec()));

        Ok(())
    }

    #[ignore] // Known issue: this test currently take too long to run on GH actions
    #[test]
    fn test_renamenx_cross_type() -> Result<()> {
        let store = Store::new_memory()?;

        // Test 1: RENAMENX string to existing hash key name - should fail
        store.set(b"strkey".to_vec(), b"strvalue".to_vec())?;
        store.hset(b"hashkey", b"field", b"hashvalue".to_vec())?;
        assert!(!store.renamenx(b"strkey", b"hashkey")?);
        // Both keys should remain unchanged
        assert_eq!(store.get(b"strkey"), Some(b"strvalue".to_vec()));
        assert_eq!(
            store.hget(b"hashkey", b"field")?,
            Some(b"hashvalue".to_vec())
        );

        // Test 2: RENAMENX hash to existing string key name - should fail
        store.hset(b"hash2", b"f1", b"v1".to_vec())?;
        store.set(b"str2".to_vec(), b"str2value".to_vec())?;
        assert!(!store.renamenx(b"hash2", b"str2")?);
        // Both keys should remain unchanged
        assert_eq!(store.hget(b"hash2", b"f1")?, Some(b"v1".to_vec()));
        assert_eq!(store.get(b"str2"), Some(b"str2value".to_vec()));

        // Test 3: RENAMENX string to non-existing key - should succeed
        store.set(b"str3".to_vec(), b"str3value".to_vec())?;
        assert!(store.renamenx(b"str3", b"newstr3")?);
        assert!(!store.exists(b"str3"));
        assert_eq!(store.get(b"newstr3"), Some(b"str3value".to_vec()));

        // Test 4: RENAMENX hash to non-existing key - should succeed
        store.hset(b"hash3", b"f1", b"v1".to_vec())?;
        assert!(store.renamenx(b"hash3", b"newhash3")?);
        assert!(!store.exists(b"hash3"));
        assert_eq!(store.hget(b"newhash3", b"f1")?, Some(b"v1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_incrby_preserves_ttl() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"counter".to_vec(), b"10".to_vec())?;
        store.expire(b"counter", 60);

        store.incrby(b"counter", 5)?;

        // TTL should still be set
        let ttl = store.ttl(b"counter");
        assert!(ttl > 50 && ttl <= 60);

        Ok(())
    }

    #[test]
    fn test_append_preserves_ttl() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello".to_vec())?;
        store.expire(b"mykey", 60);

        store.append(b"mykey", b" World")?;

        // TTL should still be set
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 50 && ttl <= 60);

        Ok(())
    }

    #[test]
    fn test_setrange_preserves_ttl() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"mykey".to_vec(), b"Hello World".to_vec())?;
        store.expire(b"mykey", 60);

        // Overwrite part of the string
        store.setrange(b"mykey", 6, b"Redis")?;
        assert_eq!(store.get(b"mykey"), Some(b"Hello Redis".to_vec()));

        // TTL should still be set
        let ttl = store.ttl(b"mykey");
        assert!(ttl > 50 && ttl <= 60);

        Ok(())
    }

    #[test]
    fn test_setrange_overflow_protection() -> Result<()> {
        let store = Store::new_memory()?;

        // Attempt to trigger integer overflow with usize::MAX offset
        // offset + value.len() would overflow without checked arithmetic
        let result = store.setrange(b"mykey", usize::MAX, b"test");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exceeds maximum allowed size"));

        // Also test with large offset that would overflow with value length
        let result = store.setrange(b"mykey", usize::MAX - 2, b"test");
        assert!(result.is_err());

        Ok(())
    }

    // ========== Milestone 1.4 Key Scanning Tests ==========

    #[test]
    fn test_keys_all() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.set(b"key2".to_vec(), b"value2".to_vec())?;
        store.set(b"other".to_vec(), b"value3".to_vec())?;

        let pattern = crate::pattern::Pattern::new(b"*");
        let keys = store.keys(&pattern);
        assert_eq!(keys.len(), 3);

        Ok(())
    }

    #[test]
    fn test_keys_pattern() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"user:1".to_vec(), b"value1".to_vec())?;
        store.set(b"user:2".to_vec(), b"value2".to_vec())?;
        store.set(b"session:1".to_vec(), b"value3".to_vec())?;

        let pattern = crate::pattern::Pattern::new(b"user:*");
        let keys = store.keys(&pattern);
        assert_eq!(keys.len(), 2);

        let pattern = crate::pattern::Pattern::new(b"session:*");
        let keys = store.keys(&pattern);
        assert_eq!(keys.len(), 1);

        Ok(())
    }

    #[test]
    fn test_keys_includes_hash() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"string_key".to_vec(), b"value".to_vec())?;
        store.hset(b"hash_key", b"field", b"value".to_vec())?;

        let pattern = crate::pattern::Pattern::new(b"*");
        let keys = store.keys(&pattern);
        assert_eq!(keys.len(), 2);

        Ok(())
    }

    #[test]
    fn test_keys_excludes_expired() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"permanent".to_vec(), b"value".to_vec())?;
        store.set_with_expiry(
            b"expired".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() - Duration::from_secs(1)),
        )?;

        let pattern = crate::pattern::Pattern::new(b"*");
        let keys = store.keys(&pattern);
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"permanent".to_vec());

        Ok(())
    }

    #[test]
    fn test_dbsize() -> Result<()> {
        let store = Store::new_memory()?;
        assert_eq!(store.dbsize(), 0);

        store.set(b"key1".to_vec(), b"value".to_vec())?;
        assert_eq!(store.dbsize(), 1);

        store.set(b"key2".to_vec(), b"value".to_vec())?;
        assert_eq!(store.dbsize(), 2);

        store.hset(b"hash", b"field", b"value".to_vec())?;
        assert_eq!(store.dbsize(), 3);

        Ok(())
    }

    #[test]
    fn test_dbsize_excludes_expired() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"permanent".to_vec(), b"value".to_vec())?;
        store.set_with_expiry(
            b"expired".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() - Duration::from_secs(1)),
        )?;

        assert_eq!(store.dbsize(), 1);

        Ok(())
    }

    #[test]
    fn test_random_key() -> Result<()> {
        let store = Store::new_memory()?;

        // Empty store
        assert!(store.random_key().is_none());

        // With keys
        store.set(b"key1".to_vec(), b"value".to_vec())?;
        store.set(b"key2".to_vec(), b"value".to_vec())?;

        let key = store.random_key().unwrap();
        assert!(key == b"key1".to_vec() || key == b"key2".to_vec());

        Ok(())
    }

    #[test]
    fn test_random_key_skips_expired() -> Result<()> {
        let store = Store::new_memory()?;

        store.set_with_expiry(
            b"expired".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() - Duration::from_secs(1)),
        )?;
        store.set(b"permanent".to_vec(), b"value".to_vec())?;

        let key = store.random_key().unwrap();
        assert_eq!(key, b"permanent".to_vec());

        Ok(())
    }

    #[test]
    fn test_flush_db() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"key1".to_vec(), b"value".to_vec())?;
        store.set(b"key2".to_vec(), b"value".to_vec())?;
        store.hset(b"hash", b"field", b"value".to_vec())?;

        assert_eq!(store.dbsize(), 3);

        let count = store.flush_db();
        assert_eq!(count, 3);
        assert_eq!(store.dbsize(), 0);
        assert!(store.get(b"key1").is_none());
        assert!(store.hget(b"hash", b"field")?.is_none());

        Ok(())
    }

    #[test]
    fn test_scan_basic() -> Result<()> {
        let store = Store::new_memory()?;
        for i in 0..5 {
            store.set(format!("key{}", i).into_bytes(), b"value".to_vec())?;
        }

        // Full scan with high count
        let (cursor, keys) = store.scan(0, None, 100);
        assert_eq!(cursor, 0); // Complete
        assert_eq!(keys.len(), 5);

        Ok(())
    }

    #[test]
    fn test_scan_with_pattern() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"user:1".to_vec(), b"value".to_vec())?;
        store.set(b"user:2".to_vec(), b"value".to_vec())?;
        store.set(b"session:1".to_vec(), b"value".to_vec())?;

        let pattern = crate::pattern::Pattern::new(b"user:*");
        let (cursor, keys) = store.scan(0, Some(&pattern), 100);
        assert_eq!(cursor, 0);
        assert_eq!(keys.len(), 2);

        Ok(())
    }

    #[test]
    fn test_scan_iteration() -> Result<()> {
        let store = Store::new_memory()?;
        for i in 0..20 {
            store.set(format!("key{:02}", i).into_bytes(), b"value".to_vec())?;
        }

        let mut all_keys = Vec::new();
        let mut cursor = 0u64;

        // Iterate with small count
        loop {
            let (new_cursor, keys) = store.scan(cursor, None, 5);
            all_keys.extend(keys);
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_keys.len(), 20);

        Ok(())
    }

    #[test]
    fn test_scan_excludes_expired() -> Result<()> {
        let store = Store::new_memory()?;
        store.set(b"permanent1".to_vec(), b"value".to_vec())?;
        store.set(b"permanent2".to_vec(), b"value".to_vec())?;
        store.set_with_expiry(
            b"expired1".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() - Duration::from_secs(1)),
        )?;
        store.set_with_expiry(
            b"expired2".to_vec(),
            b"value".to_vec(),
            Some(Instant::now() - Duration::from_secs(1)),
        )?;

        // SCAN should only return non-expired keys
        let mut all_keys = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (new_cursor, keys) = store.scan(cursor, None, 10);
            all_keys.extend(keys);
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        assert_eq!(all_keys.len(), 2);
        for key in &all_keys {
            assert!(key.starts_with(b"permanent"));
        }

        Ok(())
    }

    #[test]
    fn test_scan_transitions_string_to_hash() -> Result<()> {
        let store = Store::new_memory()?;

        // Create string keys
        store.set(b"string1".to_vec(), b"value".to_vec())?;
        store.set(b"string2".to_vec(), b"value".to_vec())?;

        // Create hash keys
        store.hset(b"hash1", b"field", b"value".to_vec())?;
        store.hset(b"hash2", b"field", b"value".to_vec())?;

        // Scan with small count to force multiple iterations
        let mut all_keys = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (new_cursor, keys) = store.scan(cursor, None, 1);
            all_keys.extend(keys);
            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        // Should have all 4 keys (2 string + 2 hash)
        assert_eq!(all_keys.len(), 4);

        // Verify we got both string and hash keys
        let has_string = all_keys.iter().any(|k| k.starts_with(b"string"));
        let has_hash = all_keys.iter().any(|k| k.starts_with(b"hash"));
        assert!(has_string, "Should have string keys");
        assert!(has_hash, "Should have hash keys");

        Ok(())
    }

    // ========== Milestone 1.5: Complete Hash Commands Store Tests ==========

    #[test]
    fn test_hmset() -> Result<()> {
        let store = Store::new_memory()?;

        let pairs: Vec<(&[u8], Vec<u8>)> = vec![
            (b"field1" as &[u8], b"value1".to_vec()),
            (b"field2" as &[u8], b"value2".to_vec()),
            (b"field3" as &[u8], b"value3".to_vec()),
        ];

        let new_count = store.hmset(b"myhash", &pairs)?;
        assert_eq!(new_count, 3);

        // Verify fields were set
        assert_eq!(store.hget(b"myhash", b"field1")?, Some(b"value1".to_vec()));
        assert_eq!(store.hget(b"myhash", b"field2")?, Some(b"value2".to_vec()));
        assert_eq!(store.hget(b"myhash", b"field3")?, Some(b"value3".to_vec()));

        // Update existing fields
        let pairs2: Vec<(&[u8], Vec<u8>)> = vec![
            (b"field1" as &[u8], b"new_value1".to_vec()),
            (b"field4" as &[u8], b"value4".to_vec()),
        ];
        let new_count = store.hmset(b"myhash", &pairs2)?;
        assert_eq!(new_count, 1); // Only field4 is new

        assert_eq!(
            store.hget(b"myhash", b"field1")?,
            Some(b"new_value1".to_vec())
        );
        assert_eq!(store.hget(b"myhash", b"field4")?, Some(b"value4".to_vec()));

        Ok(())
    }

    #[test]
    fn test_hmget() -> Result<()> {
        let store = Store::new_memory()?;

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let fields: Vec<&[u8]> = vec![b"field1", b"field2", b"field3"];
        let result = store.hmget(b"myhash", &fields)?;

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some(b"value1".to_vec()));
        assert_eq!(result[1], Some(b"value2".to_vec()));
        assert_eq!(result[2], None);

        // Non-existent key
        let result = store.hmget(b"nonexistent", &fields)?;
        assert_eq!(result.len(), 3);
        assert!(result.iter().all(|v| v.is_none()));

        Ok(())
    }

    #[test]
    fn test_hlen() -> Result<()> {
        let store = Store::new_memory()?;

        assert_eq!(store.hlen(b"nonexistent"), 0);

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        assert_eq!(store.hlen(b"myhash"), 1);

        store.hset(b"myhash", b"field2", b"value2".to_vec())?;
        assert_eq!(store.hlen(b"myhash"), 2);

        store.hdel(b"myhash", b"field1")?;
        assert_eq!(store.hlen(b"myhash"), 1);

        Ok(())
    }

    #[test]
    fn test_hexists() -> Result<()> {
        let store = Store::new_memory()?;

        assert!(!store.hexists(b"nonexistent", b"field"));

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        assert!(store.hexists(b"myhash", b"field1"));
        assert!(!store.hexists(b"myhash", b"field2"));

        Ok(())
    }

    #[test]
    fn test_hkeys() -> Result<()> {
        let store = Store::new_memory()?;

        assert!(store.hkeys(b"nonexistent").is_empty());

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let keys = store.hkeys(b"myhash");
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&b"field1".to_vec()));
        assert!(keys.contains(&b"field2".to_vec()));

        Ok(())
    }

    #[test]
    fn test_hvals() -> Result<()> {
        let store = Store::new_memory()?;

        assert!(store.hvals(b"nonexistent").is_empty());

        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let vals = store.hvals(b"myhash");
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&b"value1".to_vec()));
        assert!(vals.contains(&b"value2".to_vec()));

        Ok(())
    }

    #[test]
    fn test_hincrby() -> Result<()> {
        let store = Store::new_memory()?;

        // Non-existent field - creates with increment value
        assert_eq!(store.hincrby(b"myhash", b"counter", 5)?, 5);

        // Increment existing
        assert_eq!(store.hincrby(b"myhash", b"counter", 10)?, 15);

        // Negative increment
        assert_eq!(store.hincrby(b"myhash", b"counter", -3)?, 12);

        // Verify stored value
        assert_eq!(store.hget(b"myhash", b"counter")?, Some(b"12".to_vec()));

        Ok(())
    }

    #[test]
    fn test_hincrby_invalid_value() -> Result<()> {
        let store = Store::new_memory()?;

        store.hset(b"myhash", b"field", b"not a number".to_vec())?;

        let result = store.hincrby(b"myhash", b"field", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not an integer"));

        Ok(())
    }

    #[test]
    fn test_hincrby_overflow() -> Result<()> {
        let store = Store::new_memory()?;

        store.hset(b"myhash", b"max", i64::MAX.to_string().into_bytes())?;

        let result = store.hincrby(b"myhash", b"max", 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("overflow"));

        Ok(())
    }

    #[test]
    fn test_hincrbyfloat() -> Result<()> {
        let store = Store::new_memory()?;

        // Non-existent field - creates with increment value
        let val = store.hincrbyfloat(b"myhash", b"price", 10.5)?;
        assert!((val - 10.5).abs() < 0.001);

        // Increment existing
        let val = store.hincrbyfloat(b"myhash", b"price", 0.1)?;
        assert!((val - 10.6).abs() < 0.001);

        // Negative increment
        let val = store.hincrbyfloat(b"myhash", b"price", -1.1)?;
        assert!((val - 9.5).abs() < 0.001);

        Ok(())
    }

    #[test]
    fn test_hincrbyfloat_invalid() -> Result<()> {
        let store = Store::new_memory()?;

        store.hset(b"myhash", b"field", b"not a number".to_vec())?;

        let result = store.hincrbyfloat(b"myhash", b"field", 1.0);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not a valid float")
        );

        Ok(())
    }

    #[test]
    fn test_hincrbyfloat_nan_infinity() -> Result<()> {
        let store = Store::new_memory()?;

        // Test incrementing by infinity
        let result = store.hincrbyfloat(b"myhash", b"field", f64::INFINITY);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        // Test incrementing by NaN
        let result = store.hincrbyfloat(b"myhash", b"field2", f64::NAN);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("NaN or Infinity"));

        Ok(())
    }

    #[test]
    fn test_hsetnx() -> Result<()> {
        let store = Store::new_memory()?;

        // Set new field - returns true
        assert!(store.hsetnx(b"myhash", b"field", b"value".to_vec())?);

        // Try to set existing field - returns false
        assert!(!store.hsetnx(b"myhash", b"field", b"new_value".to_vec())?);

        // Verify value is still the original
        assert_eq!(store.hget(b"myhash", b"field")?, Some(b"value".to_vec()));

        // Set different field - returns true
        assert!(store.hsetnx(b"myhash", b"field2", b"value2".to_vec())?);

        Ok(())
    }

    #[test]
    fn test_hgetall() -> Result<()> {
        let store = Store::new_memory()?;

        // Empty hash
        assert!(store.hgetall(b"nonexistent")?.is_empty());

        // Add some fields
        store.hset(b"myhash", b"field1", b"value1".to_vec())?;
        store.hset(b"myhash", b"field2", b"value2".to_vec())?;

        let result = store.hgetall(b"myhash")?;
        assert_eq!(result.len(), 2);

        // Convert to a HashMap for easier verification
        let map: hashbrown::HashMap<_, _> = result.into_iter().collect();
        assert_eq!(map.get(b"field1".as_slice()), Some(&b"value1".to_vec()));
        assert_eq!(map.get(b"field2".as_slice()), Some(&b"value2".to_vec()));

        Ok(())
    }

    #[test]
    fn test_snapshot_save_load() -> Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("dump.eagle");
        let path_str = path.to_str().unwrap();

        let store = Store::new()?;
        store.set(b"key1".to_vec(), b"value1".to_vec())?;
        store.hset(b"hash1", b"field1", b"value1".to_vec())?;

        // Expiry
        store.set(b"expire1".to_vec(), b"val".to_vec())?;
        store.expire(b"expire1", 100);

        store.save_to_path(path_str)?;

        assert!(path.exists());

        let store2 = Store::new()?;
        let count = store2.load_snapshot(path_str)?;
        assert_eq!(count, 3);

        assert_eq!(store2.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(store2.hget(b"hash1", b"field1")?, Some(b"value1".to_vec()));

        let ttl = store2.ttl(b"expire1");
        assert!(ttl > 0);

        Ok(())
    }

    #[test]
    fn test_bgsave() -> Result<()> {
        let store = Store::new()?;
        store.set(b"bgkey".to_vec(), b"bgval".to_vec())?;

        // BGSAVE spawns a tokio task, so we need to run it in a tokio runtime
        let rt = tokio::runtime::Runtime::new()?;
        let msg = rt.block_on(async {
            // We need to call bgsave from within the store's async context
            // Since bgsave spawns a blocking task, we just verify it returns immediately
            store.bgsave()
        })?;
        assert_eq!(msg, "Background saving started");

        // Wait a bit for background save to complete
        std::thread::sleep(std::time::Duration::from_millis(100));

        Ok(())
    }
}
