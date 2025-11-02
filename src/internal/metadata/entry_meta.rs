#[derive(Debug, Clone, Default)]
pub struct EntryMeta {
    /// 源文件路径，相对于仓库根
    pub file_path: Option<String>,

    /// 所在 pack 文件 ID 或索引号
    pub pack_id: Option<String>,

    /// 在 pack 文件中的偏移量
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
