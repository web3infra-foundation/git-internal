/// Git Protocol Module
///
/// This module provides a clean, minimal, and transport-agnostic Git smart protocol implementation.
/// It abstracts away the complexities of different transport layers (HTTP, SSH) and provides
/// a unified interface for Git operations.
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
