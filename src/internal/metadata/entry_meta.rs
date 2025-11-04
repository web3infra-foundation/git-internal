#[derive(Debug, Clone, Default)]
pub struct EntryMeta {
    pub file_path: Option<String>,

    pub pack_id: Option<String>,

    /// Offset within the pack file
    pub pack_offset: Option<usize>,

    pub is_delta: Option<bool>,
}

impl EntryMeta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_pack_id(&mut self, id: impl Into<String>) -> &mut Self {
        self.pack_id = Some(id.into());
        self
    }
}
