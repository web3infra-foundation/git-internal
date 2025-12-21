//! Error variants for the custom delta encoder/decoder so callers can surface friendly failures.

use thiserror::Error;

/// Delta encoder/decoder error kinds exposed to callers.
#[derive(Error, Debug)]
pub enum GitDeltaError {
    /// Failure while building delta instructions.
    #[error("Delta encoder error: {0}")]
    DeltaEncoderError(String),

    /// Failure while applying delta instructions.
    #[error("Delta decoder error: {0}")]
    DeltaDecoderError(String),
}
