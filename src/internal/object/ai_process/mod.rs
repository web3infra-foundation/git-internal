//! AI-powered processes for Git internals.
//! This module provides abstractions and implementations for integrating AI agents
//! into Git workflows, enabling intelligent automation and assistance in handling Git objects
//! and operations.
pub mod base;
pub mod checksum;
pub mod objects;

pub use base::*;
pub use checksum::*;
pub use objects::*;
