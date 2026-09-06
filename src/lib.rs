//! holler-client library root.
//!
//! CLI/binary and per-story modules land here as issues #23-#32 are
//! implemented. This crate currently has no runtime behavior.

pub mod acp_driver;
pub mod config;
pub mod connection;
pub mod credential;
pub mod debug;
pub mod join;
pub mod proto;
pub mod server_address;
pub mod session_manager;
pub mod status;
