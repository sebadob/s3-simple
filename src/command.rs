use crate::constants::EMPTY_PAYLOAD_SHA;
use crate::types::Multipart;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Debug, Serialize)]
pub struct Part {
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    #[serde(rename = "ETag")]
    pub etag: String,
}

impl fmt::Display for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
            self.part_number, self.etag
        )
    }
}

#[derive(Debug)]
pub struct CompleteMultipartUploadData {
    pub parts: Vec<Part>,
}

impl fmt::Display for CompleteMultipartUploadData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<CompleteMultipartUpload>")?;
        for part in &self.parts {
            write!(f, "{}", part)?;
        }
        write!(f, "</CompleteMultipartUpload>")
    }
}

impl CompleteMultipartUploadData {
    pub fn len(&self) -> usize {
        self.to_string().as_bytes().len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.to_string().as_bytes().is_empty()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum Command<'a> {
    HeadObject,
    CopyObject {
        from: &'a str,
    },
    DeleteObject,
    DeleteObjectTagging,
    GetObject,
    GetObjectRange {
        start: u64,
        end: Option<u64>,
    },
    GetObjectTagging,
    PutObject {
        content: &'a [u8],
        content_type: &'a str,
        multipart: Option<Multipart<'a>>,
    },
    PutObjectTagging {
        tags: &'a str,
    },
    ListMultipartUploads {
        prefix: Option<&'a str>,
        delimiter: Option<&'a str>,
        key_marker: Option<String>,
        max_uploads: Option<usize>,
    },
    ListObjects {
        prefix: &'a str,
        delimiter: Option<&'a str>,
        marker: Option<String>,
        max_keys: Option<usize>,
    },
    ListObjectsV2 {
        prefix: &'a str,
        delimiter: Option<&'a str>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<usize>,
    },
    GetBucketLocation,
    // PresignGet {
    //     expiry_secs: u32,
    //     custom_queries: Option<HashMap<String, String>>,
    // },
    // PresignPut {
    //     expiry_secs: u32,
    //     custom_headers: Option<HeaderMap>,
    // },
    // PresignPost {
    //     expiry_secs: u32,
    //     post_policy: String,
    // },
    // PresignDelete {
    //     expiry_secs: u32,
    // },
    InitiateMultipartUpload {
        content_type: &'a str,
    },
    UploadPart {
        part_number: u32,
        content: &'a [u8],
        upload_id: &'a str,
    },
    AbortMultipartUpload {
        upload_id: &'a str,
    },
    CompleteMultipartUpload {
        upload_id: &'a str,
        data: CompleteMultipartUploadData,
    },
}

impl<'a> Command<'a> {
    pub(crate) fn http_method(&self) -> http::Method {
        match *self {
            Command::GetObject
            | Command::GetObjectRange { .. }
            | Command::ListObjects { .. }
            | Command::ListObjectsV2 { .. }
            | Command::GetBucketLocation
            | Command::GetObjectTagging
            | Command::ListMultipartUploads { .. } => http::Method::GET,
            Command::PutObject { .. }
            | Command::CopyObject { from: _ }
            | Command::PutObjectTagging { .. }
            | Command::UploadPart { .. } => http::Method::PUT,
            Command::DeleteObject
            | Command::DeleteObjectTagging
            | Command::AbortMultipartUpload { .. } => http::Method::DELETE,
            Command::InitiateMultipartUpload { .. } | Command::CompleteMultipartUpload { .. } => {
                http::Method::POST
            }
            Command::HeadObject => http::Method::HEAD,
        }
    }

    pub(crate) fn content_length(&self) -> usize {
        match &self {
            Command::PutObject { content, .. } => content.len(),
            Command::PutObjectTagging { tags } => tags.len(),
            Command::UploadPart { content, .. } => content.len(),
            Command::CompleteMultipartUpload { data, .. } => data.len(),
            _ => 0,
        }
    }

    pub(crate) fn content_type(&self) -> &str {
        match self {
            Command::InitiateMultipartUpload { content_type } => content_type,
            Command::PutObject { content_type, .. } => content_type,
            Command::CompleteMultipartUpload { .. } => "application/xml",
            _ => "text/plain",
        }
    }

    pub(crate) fn sha256(&self) -> String {
        match &self {
            Command::PutObject { content, .. } => {
                let mut sha = Sha256::default();
                sha.update(content);
                hex::encode(sha.finalize().as_slice())
            }
            Command::PutObjectTagging { tags } => {
                let mut sha = Sha256::default();
                sha.update(tags.as_bytes());
                hex::encode(sha.finalize().as_slice())
            }
            Command::CompleteMultipartUpload { data, .. } => {
                let mut sha = Sha256::default();
                sha.update(data.to_string().as_bytes());
                hex::encode(sha.finalize().as_slice())
            }
            _ => EMPTY_PAYLOAD_SHA.into(),
        }
    }
}
