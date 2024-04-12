// Copyright 2024 Sebastian Dobe <sebastiandobe@mailbox.org>

#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]

use base64::engine::general_purpose;
use base64::Engine;
use std::env;

pub use crate::bucket::{Bucket, BucketOptions};
pub use crate::credentials::{AccessKeyId, AccessKeySecret, Credentials};
pub use crate::error::S3Error;
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

#[derive(Debug, Clone)]
pub struct Region(String);

impl Region {
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
