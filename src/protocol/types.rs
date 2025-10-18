use std::fmt;
use std::str::FromStr;

/// Protocol error types
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("Invalid service: {0}")]
    InvalidService(String),

    #[error("Repository not found: {0}")]
    RepositoryNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Pack error: {0}")]
    Pack(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl ProtocolError {
    pub fn invalid_service(service: &str) -> Self {
        ProtocolError::InvalidService(service.to_string())
    }

    pub fn repository_error(msg: String) -> Self {
        ProtocolError::Internal(msg)
    }

    pub fn invalid_request(msg: &str) -> Self {
        ProtocolError::InvalidRequest(msg.to_string())
    }

    pub fn unauthorized(msg: &str) -> Self {
        ProtocolError::Unauthorized(msg.to_string())
    }
}

/// Git transport protocol types
#[derive(Debug, PartialEq, Clone, Copy, Default)]
pub enum TransportProtocol {
    Local,
    #[default]
    Http,
    Ssh,
    Git,
    P2p,
}

/// Git service types for smart protocol
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ServiceType {
    UploadPack,
    ReceivePack,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServiceType::UploadPack => write!(f, "git-upload-pack"),
            ServiceType::ReceivePack => write!(f, "git-receive-pack"),
        }
    }
}

impl FromStr for ServiceType {
    type Err = ProtocolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "git-upload-pack" => Ok(ServiceType::UploadPack),
            "git-receive-pack" => Ok(ServiceType::ReceivePack),
            _ => Err(ProtocolError::InvalidService(s.to_string())),
        }
    }
}

/// Git protocol capabilities
#[derive(Debug, Clone, PartialEq)]
pub enum Capability {
    MultiAck,
    MultiAckDetailed,
    NoDone,
    SideBand,
    SideBand64k,
    ReportStatus,
    ReportStatusv2,
    OfsDelta,
    DeepenSince,
    DeepenNot,
}

impl FromStr for Capability {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "multi_ack" => Ok(Capability::MultiAck),
            "multi_ack_detailed" => Ok(Capability::MultiAckDetailed),
            "no-done" => Ok(Capability::NoDone),
            "side-band" => Ok(Capability::SideBand),
            "side-band-64k" => Ok(Capability::SideBand64k),
            "report-status" => Ok(Capability::ReportStatus),
            "report-status-v2" => Ok(Capability::ReportStatusv2),
            "ofs-delta" => Ok(Capability::OfsDelta),
            "deepen-since" => Ok(Capability::DeepenSince),
            "deepen-not" => Ok(Capability::DeepenNot),
            _ => Err(()),
        }
    }
}

/// Side-band types for multiplexed data streams
pub enum SideBind {
    /// Sideband 1 contains packfile data
    PackfileData,
    /// Sideband 2 contains progress information
    ProgressInfo,
    /// Sideband 3 contains error information
    Error,
}

impl SideBind {
    pub fn value(&self) -> u8 {
        match self {
            Self::PackfileData => b'\x01',
            Self::ProgressInfo => b'\x02',
            Self::Error => b'\x03',
        }
    }
}

/// Reference types in Git
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RefTypeEnum {
    Branch,
    Tag,
}

/// Git reference information
#[derive(Clone, Debug)]
pub struct GitRef {
    pub name: String,
    pub hash: String,
}

/// Reference command for push operations
#[derive(Debug, Clone)]
pub struct RefCommand {
    pub old_hash: String,
    pub new_hash: String,
    pub ref_name: String,
    pub ref_type: RefTypeEnum,
    pub default_branch: bool,
    pub status: CommandStatus,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub enum CommandStatus {
    Pending,
    Success,
    Failed,
}

impl RefCommand {
    pub fn new(old_hash: String, new_hash: String, ref_name: String) -> Self {
        // Determine ref type based on ref name
        let ref_type = if ref_name.starts_with("refs/tags/") {
            RefTypeEnum::Tag
        } else {
            RefTypeEnum::Branch
        };

        Self {
            old_hash,
            new_hash,
            ref_name,
            ref_type,
            default_branch: false,
            status: CommandStatus::Pending,
            error_message: None,
        }
    }

    pub fn failed(&mut self, error: String) {
        self.status = CommandStatus::Failed;
        self.error_message = Some(error);
    }

    pub fn success(&mut self) {
        self.status = CommandStatus::Success;
        self.error_message = None;
    }

    pub fn get_status(&self) -> String {
        match &self.status {
            CommandStatus::Success => format!("ok {}", self.ref_name),
            CommandStatus::Failed => {
                let error = self.error_message.as_deref().unwrap_or("unknown error");
                format!("ng {} {}", self.ref_name, error)
            }
            CommandStatus::Pending => format!("ok {}", self.ref_name), // Default to ok for pending
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum CommandType {
    Create,
    Update,
    Delete,
}

/// Zero object ID constant
pub const ZERO_ID: &str = "0000000000000000000000000000000000000000";

/// Protocol constants
pub const LF: char = '\n';
pub const SP: char = ' ';
pub const NUL: char = '\0';
pub const PKT_LINE_END_MARKER: &[u8; 4] = b"0000";

// Git protocol capability lists
pub const RECEIVE_CAP_LIST: &str =
    "report-status report-status-v2 delete-refs quiet atomic no-thin ";
pub const COMMON_CAP_LIST: &str = "side-band-64k ofs-delta agent=git-internal/0.1.0";
pub const UPLOAD_CAP_LIST: &str = "multi_ack_detailed no-done include-tag ";
