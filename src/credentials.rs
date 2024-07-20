#![allow(dead_code)]

use crate::error::S3Error;
use std::env;
use std::fmt::{Debug, Formatter};

#[derive(Debug, Clone)]
pub struct AccessKeyId(pub String);

impl AsRef<str> for AccessKeyId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl AccessKeyId {
    pub fn new(access_key_id: String) -> Self {
        Self(access_key_id)
    }
}

#[derive(Clone)]
pub struct AccessKeySecret(pub String);

impl Debug for AccessKeySecret {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AccessKeySecret(<hidden>)")
    }
}

impl AsRef<str> for AccessKeySecret {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl AccessKeySecret {
    pub fn new(access_key_secret: String) -> Self {
        Self(access_key_secret)
    }
}

#[derive(Debug, Clone)]
pub struct Credentials {
    pub access_key_id: AccessKeyId,
    pub access_key_secret: AccessKeySecret,
}

impl Credentials {
    pub fn new<S>(key: S, secret: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            access_key_id: AccessKeyId(key.into()),
            access_key_secret: AccessKeySecret(secret.into()),
        }
    }

    pub fn try_from_env() -> Result<Self, S3Error> {
        let access_key_id = env::var("S3_ACCESS_KEY_ID")?;
        let access_key_secret = env::var("S3_ACCESS_KEY_SECRET")?;

        Ok(Self {
            access_key_id: AccessKeyId(access_key_id),
            access_key_secret: AccessKeySecret(access_key_secret),
        })
    }
}
