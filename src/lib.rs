// Copyright 2024 Sebastian Dobe <sebastiandobe@mailbox.org>

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use base64::engine::general_purpose;
use base64::Engine;
use std::env;

/// S3 Bucket operations, your main entrypoint
pub use crate::bucket::{Bucket};
/// Custom options for bucket connections
pub use crate::bucket::{BucketOptions};
/// S3 Credentials
pub use crate::credentials::{AccessKeyId, AccessKeySecret, Credentials};
/// Specialized S3 Error type which wraps errors from different sources
pub use crate::error::S3Error;
/// Specialized Response objects
pub use crate::types::{HeadObjectResult, Object, PutStreamResponse};
pub use reqwest::Response as S3Response;
pub use reqwest::StatusCode as S3StatusCode;

mod bucket;
mod command;
mod constants;
mod credentials;
mod error;
mod signature;
mod types;

/// S3 Region Wrapper
#[derive(Debug, Clone)]
pub struct Region(pub String);

impl Region {
    pub fn new<S>(region: S) -> Self
    where
        S: Into<String>,
    {
       Self(region.into())
    }

    pub fn try_from_env() -> Result<Self, S3Error> {
        Ok(Self(env::var("S3_REGION")?))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

fn md5_url_encode(s: &[u8]) -> String {
    general_purpose::STANDARD.encode(md5::compute(s).as_ref())
}
