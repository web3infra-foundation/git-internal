//! Lightweight metadata plumbing that allows pack entries to carry auxiliary information (paths,
//! pack offsets, CRC32, etc.) through encode/decode pipelines without polluting core types.

mod entry_meta;
use std::fmt;

pub use entry_meta::EntryMeta;

/// Trait for types that can carry metadata.
pub trait MetadataExt {
    type Meta: Clone + std::fmt::Debug + Send + Sync + 'static;

    fn metadata(&self) -> Option<&Self::Meta>;
    fn metadata_mut(&mut self) -> Option<&mut Self::Meta>;
    fn set_metadata(&mut self, meta: Self::Meta);
    //fn clear_metadata(&mut self);
}

/// Wrapper type that attaches metadata to an inner type.
#[derive(Debug, Clone)]
pub struct MetaAttached<T, M> {
    pub inner: T,
    pub meta: M,
}

impl<T, M> MetadataExt for MetaAttached<T, M>
where
    M: Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type Meta = M;

    fn metadata(&self) -> Option<&Self::Meta> {
        Some(&self.meta)
    }

    fn metadata_mut(&mut self) -> Option<&mut Self::Meta> {
        Some(&mut self.meta)
    }

    fn set_metadata(&mut self, meta: Self::Meta) {
        self.meta = meta;
    }

    // fn clear_metadata(&mut self) {
    //
    // }
}

impl<T, M> fmt::Display for MetaAttached<T, M>
where
    T: fmt::Display,
    M: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} | meta: {:?}", self.inner, self.meta)
    }
}
