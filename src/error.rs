use thiserror::Error;

#[derive(Error, Debug)]
pub enum S3Error {
    #[error("credentials: {0}")]
    Credentials(String),
    #[error("env var missing: {0}")]
    EnvVarMissing(#[from] std::env::VarError),
    #[error("fmt error: {0}")]
    FmtError(#[from] std::fmt::Error),
    #[error("from utf8: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),
    #[error("header to string: {0}")]
    HeaderToStr(#[from] http::header::ToStrError),
    #[error("sha2 invalid length: {0}")]
    HmacInvalidLength(#[from] sha2::digest::InvalidLength),
    #[error("S3_HOST must have a domain and not IP: '{0}'")]
    HostDomain(&'static str),
    #[error("Http request returned a non 2** code")]
    HttpFail,
    #[error("Got HTTP {0} with content '{1}'")]
    HttpFailWithBody(u16, String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(#[from] http::Error),
    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] http::header::InvalidHeaderName),
    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),
    #[error("tokio task join: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("request: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("serde xml: {0}")]
    SerdeXml(#[from] quick_xml::de::DeError),
    #[error("Time format error: {0}")]
    TimeFormatError(#[from] time::error::Format),
    #[error("url parse: {0}")]
    UrlParse(#[from] url::ParseError),
    #[error("Utf8 decoding error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}
