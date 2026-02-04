//! Persistence commands for snapshot management.
//!
//! This module implements the SAVE, BGSAVE, and LASTSAVE commands for creating
//! and managing database snapshots.
//!
//! # Commands
//!
//! - `SAVE` - Synchronously saves the database to disk, blocking until complete
//! - `BGSAVE` - Asynchronously saves the database to disk in the background
//! - `LASTSAVE` - Returns the Unix timestamp of the last successful save

use super::CommandHandler;
use anyhow::Result;
use async_trait::async_trait;
use eagle_core::error::CommandError;
use eagle_core::resp::RespValue;
use eagle_core::store::Store;

#[derive(Debug)]
pub struct Save;

#[async_trait]
impl CommandHandler for Save {
    fn name(&self) -> &'static str {
        "SAVE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        match store.save() {
            Ok(_) => Ok(RespValue::SimpleString("OK".into())),
            Err(e) => {
                if e.to_string().contains("already in progress") {
                    Ok(RespValue::Error(
                        "ERR Background save already in progress".into(),
                    ))
                } else {
                    Ok(RespValue::Error(format!("ERR {}", e)))
                }
            }
        }
    }
}

impl Save {
    pub fn parse(items: Vec<RespValue>) -> Result<Self> {
        if !items.is_empty() {
            return Err(CommandError::wrong_arity("save").into());
        }
        Ok(Save)
    }
}

#[derive(Debug)]
pub struct Bgsave;

#[async_trait]
impl CommandHandler for Bgsave {
    fn name(&self) -> &'static str {
        "BGSAVE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        match store.bgsave() {
            Ok(msg) => Ok(RespValue::SimpleString(msg)),
            Err(e) => {
                if e.to_string().contains("already in progress") {
                    Ok(RespValue::Error(
                        "ERR Background save already in progress".into(),
                    ))
                } else {
                    Ok(RespValue::Error(format!("ERR {}", e)))
                }
            }
        }
    }
}

impl Bgsave {
    pub fn parse(items: Vec<RespValue>) -> Result<Self> {
        if !items.is_empty() {
            return Err(CommandError::wrong_arity("bgsave").into());
        }
        Ok(Bgsave)
    }
}

#[derive(Debug)]
pub struct LastSave;

#[async_trait]
impl CommandHandler for LastSave {
    fn name(&self) -> &'static str {
        "LASTSAVE"
    }

    async fn execute(&self, store: &Store) -> Result<RespValue> {
        let timestamp = store.last_save();
        Ok(RespValue::Integer(timestamp as i64))
    }
}

impl LastSave {
    pub fn parse(items: Vec<RespValue>) -> Result<Self> {
        if !items.is_empty() {
            return Err(CommandError::wrong_arity("lastsave").into());
        }
        Ok(LastSave)
    }
}
