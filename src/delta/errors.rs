//! Error variants for the custom delta encoder/decoder so callers can surface friendly failures.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum GitDeltaError {
    #[error("The `{0}` is not a valid git object type.")]
    DeltaEncoderError(String),

    #[error("The `{0}` is not a valid git object type.")]
    DeltaDecoderError(String),
}
