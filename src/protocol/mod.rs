//! Git smart-protocol façade that re-exports core traits, transport adapters, capability types, and
//! helpers so embedders can speak Git over HTTP/SSH or custom transports with minimal plumbing.

pub mod core;
pub mod http;
pub mod pack;
pub mod smart;
pub mod ssh;
pub mod types;
pub mod utils;

// Re-export main interfaces
pub use core::{AuthenticationService, GitProtocol, RepositoryAccess};

pub use types::*;
