use crate::command::{Command, CompleteMultipartUploadData, Part};
use crate::constants::LONG_DATE_TIME;
use crate::credentials::Credentials;
use crate::error::S3Error;
use crate::types::Multipart;
use crate::types::{
    HeadObjectResult, InitiateMultipartUploadResponse, ListBucketResult, PutStreamResponse,
};
use crate::{md5_url_encode, signature, Region, S3Response, S3StatusCode};
use hmac::Hmac;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, DATE, HOST, RANGE};
use http::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Response;
use sha2::digest::Mac;
use sha2::Sha256;
use std::fmt::Write;
use std::sync::OnceLock;
use std::time::Duration;
use std::{env, mem};
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, error};
use url::Url;

static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8 MiB, min for S3 is 5MiB

#[derive(Debug)]
pub struct BucketOptions {
    pub path_style: bool,
    pub list_objects_v2: bool,
}

impl Default for BucketOptions {
    fn default() -> Self {
        Self {
            path_style: env::var("S3_PATH_STYLE")
                .unwrap_or_else(|_| "false".to_string())
                .parse::<bool>()
                .expect("S3_PATH_STYLE cannot be parsed as bool"),
            list_objects_v2: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub host: Url,
    pub name: String,
    pub region: Region,
    pub credentials: Credentials,
    path_style: bool,
    list_objects_v2: bool,
}

#[allow(dead_code)]
impl Bucket {
    fn host_domain(&self) -> String {
        match self.host.domain() {
            None => {
                // in this case, we have an IP as part of the domain
                let host_str = self
                    .host
                    .host_str()
                    .expect("host_str to exist when domain does not");
                if let Some(port) = self.host.port() {
                    format!("{}:{}", host_str, port,)
                } else {
                    host_str.to_string()
                }
            }
            Some(domain) => {
                if let Some(port) = self.host.port() {
                    format!("{}:{}", domain, port)
                } else {
                    domain.to_string()
                }
            }
        }
    }

    pub fn new(
        host: Url,
        name: String,
        region: Region,
        credentials: Credentials,
        options: Option<BucketOptions>,
    ) -> Result<Self, S3Error> {
        let options = options.unwrap_or_default();
        Ok(Self {
            host,
            name,
            region,
            credentials,
            path_style: options.path_style,
            list_objects_v2: options.list_objects_v2,
        })
    }

    pub fn try_from_env() -> Result<Self, S3Error> {
        let host_env = env::var("S3_URL")?;
        let host = host_env.parse::<Url>()?;

        let name = env::var("S3_BUCKET")?;
        let region = Region::try_from_env()?;
        let credentials = Credentials::try_from_env()?;
        let options = BucketOptions::default();

        Ok(Self {
            host,
            name,
            region,
            credentials,
            path_style: options.path_style,
            list_objects_v2: options.list_objects_v2,
        })
    }

    /// HEAD information for an object
    pub async fn head<S: AsRef<str>>(&self, path: S) -> Result<HeadObjectResult, S3Error> {
        let res = self
            .send_request(Command::HeadObject, path.as_ref())
            .await?;
        if res.status().is_success() {
            Ok(HeadObjectResult::from(res.headers()))
        } else {
            eprintln!("{:?}", res.headers());
            Err(S3Error::HttpFailWithBody(
                res.status().as_u16(),
                res.text().await?,
            ))
        }
    }

    /// GET an object
    pub async fn get<P>(&self, path: P) -> Result<S3Response, S3Error>
    where
        P: AsRef<str>,
    {
        self.send_request(Command::GetObject, path.as_ref()).await
    }

    pub async fn get_range<S: AsRef<str>>(
        &self,
        path: S,
        start: u64,
        end: Option<u64>,
    ) -> Result<S3Response, S3Error> {
        if let Some(end) = end {
            // if start >= end {
            //     return S3Error::
            // }
            // TODO remove assertion and add a new error type
            assert!(start < end);
        }
        self.send_request(Command::GetObjectRange { start, end }, path.as_ref())
            .await
    }

    // pub async fn get_range_to_writer<T: AsyncWrite + Send + Unpin, S: AsRef<str>>(
    //     &self,
    //     path: S,
    //     start: u64,
    //     end: Option<u64>,
    //     writer: &mut T,
    // ) -> Result<S3StatusCode, S3Error> {
    //     if let Some(end) = end {
    //         assert!(start < end);
    //     }
    //
    //     // let command = Command::GetObjectRange { start, end };
    //     // let request = RequestImpl::new(self, path.as_ref(), command).await?;
    //     // request.response_data_to_writer(writer).await
    //     todo!()
    // }

    /// DELETE an object
    pub async fn delete<S: AsRef<str>>(&self, path: S) -> Result<S3Response, S3Error> {
        self.send_request(Command::DeleteObject, path.as_ref())
            .await
    }

    /// PUT an object
    pub async fn put<S: AsRef<str>>(&self, path: S, content: &[u8]) -> Result<S3Response, S3Error> {
        self.put_with_content_type(path, content, "application/octet-stream")
            .await
    }

    /// PUT an object with a specific content type
    pub async fn put_with_content_type<S: AsRef<str>>(
        &self,
        path: S,
        content: &[u8],
        content_type: &str,
    ) -> Result<S3Response, S3Error> {
        self.send_request(
            Command::PutObject {
                content,
                content_type,
                multipart: None,
            },
            path.as_ref(),
        )
        .await
    }

    /// Streaming object upload from any reader that implements `AsyncRead`
    pub async fn put_stream<R>(
        &self,
        reader: &mut R,
        path: String,
    ) -> Result<PutStreamResponse, S3Error>
    where
        R: AsyncRead + Unpin,
    {
        self.put_stream_with_content_type(reader, path, "application/octet-stream".to_string())
            .await
    }

    async fn initiate_multipart_upload(
        &self,
        path: &str,
        content_type: &str,
    ) -> Result<InitiateMultipartUploadResponse, S3Error> {
        let res = self
            .send_request(Command::InitiateMultipartUpload { content_type }, path)
            .await?;

        if res.status().is_success() {
            Ok(quick_xml::de::from_str(&res.text().await?)?)
        } else {
            Err(S3Error::HttpFailWithBody(
                res.status().as_u16(),
                res.text().await?,
            ))
        }
    }

    async fn multipart_request(
        &self,
        path: &str,
        chunk: Vec<u8>,
        part_number: u32,
        upload_id: &str,
        content_type: &str,
    ) -> Result<Response, S3Error> {
        self.send_request(
            Command::PutObject {
                // TODO switch to owned data would make sense here probably
                content: &chunk,
                multipart: Some(Multipart::new(part_number, upload_id)),
                content_type,
            },
            path,
        )
        .await
    }

    async fn complete_multipart_upload(
        &self,
        path: &str,
        upload_id: &str,
        parts: Vec<Part>,
    ) -> Result<Response, S3Error> {
        let data = CompleteMultipartUploadData { parts };
        self.send_request(Command::CompleteMultipartUpload { upload_id, data }, path)
            .await
    }

    /// Streaming object upload from any reader that implements `AsyncRead`
    #[tracing::instrument(level = "debug", skip_all, fields(path = path))]
    pub async fn put_stream_with_content_type<R>(
        &self,
        reader: &mut R,
        path: String,
        content_type: String,
    ) -> Result<PutStreamResponse, S3Error>
    where
        R: AsyncRead + Unpin,
    {
        // If the file is smaller CHUNK_SIZE, just do a regular upload,
        // Otherwise, perform a multipart upload.
        let mut first_chunk = Vec::with_capacity(CHUNK_SIZE);
        let first_chunk_size = reader
            .take(CHUNK_SIZE as u64)
            .read_to_end(&mut first_chunk)
            .await?;
        debug!("first_chunk size: {}", first_chunk.len());
        // TODO test how it behaves when the file size is exactly the chunk size
        if first_chunk_size < CHUNK_SIZE {
            debug!("first_chunk_size < CHUNK_SIZE -> doing normal PUT wihout stream");
            let res = self
                .put_with_content_type(&path, first_chunk.as_slice(), &content_type)
                .await?;

            let status_code = res.status().as_u16();
            return if res.status().is_success() {
                Ok(PutStreamResponse {
                    status_code,
                    uploaded_bytes: first_chunk_size,
                })
            } else {
                Err(S3Error::HttpFailWithBody(
                    status_code,
                    res.text().await.unwrap_or_default(),
                ))
            };
        }

        debug!("first_chunk_size > CHUNK_SIZE -> initiate streaming upload");

        // At this point, the file exceeds the CHUNK_SIZE.
        // This means we will upload at least 2 chunks.
        // To optimize the performance, the writer will be spawned on a dedicated
        // tokio top level tasks to make optimal use of multiple cores.
        // The very little cloned data is worth it to get better throughput.
        // A channel with 2-chunk buffer will be used for the communication to
        // get optimal performance out of the slower in / out pipelines.
        let (tx, rx) = flume::bounded(2);

        // Writer task
        let slf = self.clone();
        let handle_writer = tokio::spawn(async move {
            debug!("writer task has been started");

            let msg = slf.initiate_multipart_upload(&path, &content_type).await?;
            debug!("{:?}", msg);
            let path = msg.key;
            let upload_id = &msg.upload_id;

            let mut part_number: u32 = 0;
            let mut etags = Vec::new();

            let mut total_size = 0;
            loop {
                let chunk = if part_number == 0 {
                    // this memory swap avoids a clone of the first chunk
                    let mut bytes = Vec::default();
                    mem::swap(&mut first_chunk, &mut bytes);
                    bytes
                } else {
                    // TODO how does this behave, when chunk size and file size line up exactly?
                    // -> most probably error out -> catch it manually
                    match rx.recv_async().await {
                        Ok(Some(chunk)) => chunk,
                        Ok(None) => {
                            debug!("no more parts available in reader - finishing upload");
                            break;
                        }
                        Err(err) => {
                            debug!("chunk reader channel has been closed: {}", err);
                            // In this case, the reader has been closed. We either had an error
                            // (how to catch this?) or all bytes have been read.
                            break;
                        }
                    }
                };
                debug!("chunk size in loop {}: {}", part_number + 1, chunk.len());

                total_size += chunk.len();
                let is_last_chunk = chunk.len() < CHUNK_SIZE;

                // chunk upload
                part_number += 1;
                let res = slf
                    .multipart_request(&path, chunk, part_number, upload_id, &content_type)
                    .await?;

                if !res.status().is_success() {
                    // if chunk upload failed - abort the upload
                    match slf.abort_upload(&path, upload_id).await {
                        Ok(_) => {
                            return Err(S3Error::HttpFailWithBody(
                                res.status().as_u16(),
                                res.text().await?,
                            ));
                        }
                        Err(error) => {
                            return Err(error);
                        }
                    }
                }

                let etag = res
                    .headers()
                    .get("etag")
                    .expect("ETag in multipart response headers")
                    .to_str()
                    .expect("ETag to convert to str successfully");
                etags.push(etag.to_string());

                // TODO this is probably not the most reliable way of doing it.
                // -> filesize + chunk size exact match would fail
                if is_last_chunk {
                    debug!("is_last_chunk: {}", is_last_chunk);
                    break;
                }
            }
            debug!(
                "multipart uploading finished after {} parts with total size of {} bytes",
                part_number, total_size
            );

            // Finish the upload
            let inner_data = etags
                .into_iter()
                .enumerate()
                .map(|(i, etag)| Part {
                    etag,
                    part_number: i as u32 + 1,
                })
                .collect::<Vec<Part>>();
            debug!("data for multipart finishing: {:?}", inner_data);
            let res = slf
                .complete_multipart_upload(&path, &msg.upload_id, inner_data)
                .await?;

            if res.status().is_success() {
                Ok(PutStreamResponse {
                    status_code: res.status().as_u16(),
                    uploaded_bytes: total_size,
                })
            } else {
                Err(S3Error::HttpFailWithBody(
                    res.status().as_u16(),
                    res.text().await?,
                ))
            }
        });

        // The reader will run in this task for simplifying lifetimes
        loop {
            let mut buf = Vec::with_capacity(CHUNK_SIZE);
            match reader.take(CHUNK_SIZE as u64).read_to_end(&mut buf).await {
                Ok(size) => {
                    if size == 0 {
                        debug!("stream reader finished reading");
                        if let Err(err) = tx.send_async(None).await {
                            error!("sending the 'no more data' message in reader: {}", err);
                        }
                        break;
                    }

                    debug!("stream reader read {} bytes", size);
                    if let Err(err) = tx.send_async(Some(buf)).await {
                        error!(
                            "Stream Writer has been closed before reader finished: {}",
                            err
                        );
                        break;
                    }
                }
                Err(err) => {
                    // TODO make sure we don't get here when the chunks meet the
                    // file size exactly, because in this case a simple < check will not work
                    // for identifying the last element.
                    error!("stream reader error: {}", err);
                    break;
                }
            }
        }

        // handle_reader.await?;
        handle_writer.await?
    }

    // fn tags_into_xml<S: AsRef<str>>(&self, tags: &[(S, S)]) -> String {
    //     let mut s = String::new();
    //     let content = tags
    //         .iter()
    //         .map(|(name, value)| {
    //             format!(
    //                 "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
    //                 name.as_ref(),
    //                 value.as_ref()
    //             )
    //         })
    //         .fold(String::new(), |mut a, b| {
    //             a.push_str(b.as_str());
    //             a
    //         });
    //     s.push_str("<Tagging><TagSet>");
    //     s.push_str(&content);
    //     s.push_str("</TagSet></Tagging>");
    //     s
    // }
    //
    // pub async fn put_tagging<S: AsRef<str>>(
    //     &self,
    //     path: &str,
    //     tags: &[(S, S)],
    // ) -> Result<S3Response, S3Error> {
    //     let content = self.tags_into_xml(tags);
    //     self.send_request(Command::PutObjectTagging { tags: &content }, path.as_ref())
    //         .await
    // }
    //
    // pub async fn delete_tagging<S: AsRef<str>>(&self, path: S) -> Result<S3Response, S3Error> {
    //     self.send_request(Command::DeleteObjectTagging, path.as_ref())
    //         .await
    // }

    async fn list_page(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
        continuation_token: Option<String>,
        start_after: Option<String>,
        max_keys: Option<usize>,
    ) -> Result<(ListBucketResult, u16), S3Error> {
        let command = if self.list_objects_v2 {
            Command::ListObjectsV2 {
                prefix,
                delimiter,
                continuation_token,
                start_after,
                max_keys,
            }
        } else {
            // In the v1 ListObjects request, there is only one "marker"
            // field that serves as both the initial starting position,
            // and as the continuation token.
            Command::ListObjects {
                prefix,
                delimiter,
                marker: std::cmp::max(continuation_token, start_after),
                max_keys,
            }
        };

        let resp = self.send_request(command, "/").await?;
        let status_code = resp.status().as_u16();
        let list_bucket_result = quick_xml::de::from_reader(resp.bytes().await?.as_ref())?;

        Ok((list_bucket_result, status_code))
    }

    /// List bucket contents
    pub async fn list(
        &self,
        prefix: &str,
        delimiter: Option<&str>,
    ) -> Result<Vec<ListBucketResult>, S3Error> {
        let mut results = Vec::new();
        let mut continuation_token = None;

        loop {
            let (list_bucket_result, _) = self
                .list_page(prefix, delimiter, continuation_token, None, None)
                .await?;
            continuation_token = list_bucket_result.next_continuation_token.clone();
            results.push(list_bucket_result);
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(results)
    }

    /// S3 internal copy an object from one place to another
    pub async fn copy_internal<F, T>(&self, from: F, to: T) -> Result<S3StatusCode, S3Error>
    where
        F: AsRef<str>,
        T: AsRef<str>,
    {
        let fq_from = {
            let from = from.as_ref();
            let from = from.strip_prefix('/').unwrap_or(from);
            format!("{}/{}", self.name, from)
        };
        Ok(self
            .send_request(Command::CopyObject { from: &fq_from }, to.as_ref())
            .await?
            .status())
    }

    async fn abort_upload(&self, key: &str, upload_id: &str) -> Result<(), S3Error> {
        let resp = self
            .send_request(Command::AbortMultipartUpload { upload_id }, key)
            .await?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let utf8_content = String::from_utf8(resp.bytes().await?.to_vec())?;
            Err(S3Error::HttpFailWithBody(status.as_u16(), utf8_content))
        }
    }

    async fn send_request(
        &self,
        command: Command<'_>,
        path: &str,
    ) -> Result<reqwest::Response, S3Error> {
        let url = self.build_url(&command, path)?;
        let headers = self.build_headers(&command, &url).await?;

        let builder = Self::get_client()
            .request(command.http_method(), url)
            .headers(headers);

        let res = match command {
            Command::PutObject { content, .. } => builder.body(content.to_vec()),
            Command::PutObjectTagging { tags } => builder.body(tags.to_string()),
            Command::UploadPart { content, .. } => builder.body(content.to_vec()),
            Command::CompleteMultipartUpload { ref data, .. } => {
                let body = data.to_string();
                builder.body(body)
            }
            _ => builder.body(Vec::default()),
        }
        .send()
        .await?;

        Ok(res)
    }

    // TODO we could fully remove any writer / stream fn's. When we return the Response anyway,
    // the user can freely decide to await it in memory or use streaming...
    //
    // async fn response_to_writer<T>(res: Response, writer: &mut T,) -> Result<S3StatusCode, S3Error>
    // where
    //     T: AsyncWrite + Send + Unpin
    // {
    //     let status_code = res.status();
    //     if !status_code.is_success() {
    //         // we can exit early in that case
    //         return Err(S3Error::)
    //     }
    //
    //     let mut stream = res.bytes_stream().into_stream();
    //     while let Some(item) = stream.next().await {
    //         writer.write_all(&item?).await?;
    //     }
    // }

    fn get_client<'a>() -> &'a reqwest::Client {
        CLIENT.get_or_init(|| {
            let mut builder = reqwest::Client::builder()
                .brotli(true)
                .connect_timeout(Duration::from_secs(10))
                .tcp_keepalive(Duration::from_secs(30))
                .pool_idle_timeout(Duration::from_secs(600))
                .use_rustls_tls();
            if env::var("S3_DANGER_ALLOW_INSECURE").as_deref() == Ok("true") {
                builder = builder.danger_accept_invalid_certs(true);
            }
            builder.build().unwrap()
        })
    }

    async fn build_headers(&self, command: &Command<'_>, url: &Url) -> Result<HeaderMap, S3Error> {
        let cmd_hash = command.sha256();
        let now = OffsetDateTime::now_utc();

        let mut headers = HeaderMap::with_capacity(4);

        // host header
        let domain = self.host_domain();
        if self.path_style {
            headers.insert(HOST, HeaderValue::from_str(domain.as_str())?);
        } else {
            headers.insert(
                HOST,
                HeaderValue::try_from(format!("{}.{}", self.name, domain))?,
            );
        }

        // add command specific header
        match command {
            Command::CopyObject { from } => {
                headers.insert(
                    HeaderName::from_static("x-amz-copy-source"),
                    HeaderValue::from_str(from)?,
                );
            }
            Command::ListObjects { .. } => {}
            Command::ListObjectsV2 { .. } => {}
            Command::GetObject => {}
            Command::GetObjectTagging => {}
            Command::GetBucketLocation => {}

            // Needed to make Garage work while Minio
            // seems to ignore `content-length: 0` for these
            Command::DeleteObject => {}
            Command::GetObjectRange { .. } => {}
            Command::HeadObject { .. } => {}

            _ => {
                headers.insert(
                    CONTENT_LENGTH,
                    HeaderValue::try_from(command.content_length().to_string())?,
                );
                headers.insert(CONTENT_TYPE, HeaderValue::from_str(command.content_type())?);
            }
        }

        // hash and date
        headers.insert(
            HeaderName::from_static("x-amz-content-sha256"),
            HeaderValue::from_str(&cmd_hash)?,
        );
        headers.insert(
            HeaderName::from_static("x-amz-date"),
            HeaderValue::try_from(now.format(LONG_DATE_TIME)?)?,
        );

        match command {
            Command::PutObjectTagging { tags } => {
                headers.insert(
                    HeaderName::from_static("content-md5"),
                    HeaderValue::try_from(md5_url_encode(tags.as_bytes()))?,
                );
            }
            Command::PutObject { content, .. } => {
                headers.insert(
                    HeaderName::from_static("content-md5"),
                    HeaderValue::try_from(md5_url_encode(content))?,
                );
            }
            Command::UploadPart { content, .. } => {
                headers.insert(
                    HeaderName::from_static("content-md5"),
                    HeaderValue::try_from(md5_url_encode(content))?,
                );
            }
            Command::GetObject => {
                headers.insert(ACCEPT, HeaderValue::from_static("application/octet-stream"));
            }
            Command::GetObjectRange { start, end } => {
                headers.insert(ACCEPT, HeaderValue::from_static("application/octet-stream"));

                let range = if let Some(end) = end {
                    format!("bytes={}-{}", start, end)
                } else {
                    format!("bytes={}-", start)
                };
                headers.insert(RANGE, HeaderValue::try_from(range)?);
            }
            _ => {}
        }

        // sign all the above heavers with the secret
        let canonical_request =
            signature::canonical_request(&command.http_method(), url, &headers, &cmd_hash)?;
        let string_to_sign =
            signature::string_to_sign(&now, &self.region, canonical_request.as_bytes())?;
        let signing_key =
            signature::signing_key(&now, &self.credentials.access_key_secret, &self.region)?;
        let mut hmac = Hmac::<Sha256>::new_from_slice(&signing_key)?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());
        let signed_header = signature::signed_header_string(&headers);
        let authorization = signature::authorization_header(
            &self.credentials.access_key_id,
            &now,
            &self.region,
            &signed_header,
            &signature,
        )?;
        headers.insert(AUTHORIZATION, HeaderValue::try_from(authorization)?);

        // The format of RFC2822 is somewhat malleable, so including it in
        // signed headers can cause signature mismatches. We do include the
        // X-Amz-Date header, so requests are still properly limited to a date
        // range and can't be used again e.g. reply attacks. Adding this header
        // after the generation of the Authorization header leaves it out of
        // the signed headers.
        headers.insert(DATE, HeaderValue::try_from(now.format(&Rfc2822)?)?);

        Ok(headers)
    }

    fn build_url(&self, command: &Command, path: &str) -> Result<Url, S3Error> {
        let mut url = if self.path_style {
            format!(
                "{}://{}/{}",
                self.host.scheme(),
                self.host_domain(),
                self.name,
            )
        } else {
            format!(
                "{}://{}.{}",
                self.host.scheme(),
                self.name,
                self.host_domain(),
            )
        };

        let path = if let Some(stripped) = path.strip_prefix('/') {
            stripped
        } else {
            path
        };

        url.push('/');
        url.push_str(&signature::uri_encode(path, false));

        match command {
            Command::InitiateMultipartUpload { .. } | Command::ListMultipartUploads { .. } => {
                url.push_str("?uploads")
            }
            Command::AbortMultipartUpload { upload_id } => {
                write!(url, "?uploadId={}", upload_id).expect("write! to succeed");
            }
            Command::CompleteMultipartUpload { upload_id, .. } => {
                write!(url, "?uploadId={}", upload_id).expect("write! to succeed");
            }
            Command::PutObject {
                multipart: Some(multipart),
                ..
            } => url.push_str(&multipart.query_string()),
            _ => {}
        }

        let mut url = Url::parse(&url)?;

        match command {
            Command::ListObjectsV2 {
                prefix,
                delimiter,
                continuation_token,
                start_after,
                max_keys,
            } => {
                let mut query_pairs = url.query_pairs_mut();
                if let Some(d) = delimiter {
                    query_pairs.append_pair("delimiter", d);
                }

                query_pairs.append_pair("prefix", prefix);
                query_pairs.append_pair("list-type", "2");
                if let Some(token) = continuation_token {
                    query_pairs.append_pair("continuation-token", token);
                }
                if let Some(start_after) = start_after {
                    query_pairs.append_pair("start-after", start_after);
                }
                if let Some(max_keys) = max_keys {
                    query_pairs.append_pair("max-keys", &max_keys.to_string());
                }
            }

            Command::ListObjects {
                prefix,
                delimiter,
                marker,
                max_keys,
            } => {
                let mut query_pairs = url.query_pairs_mut();
                if let Some(d) = delimiter {
                    query_pairs.append_pair("delimiter", d);
                }

                query_pairs.append_pair("prefix", prefix);
                if let Some(marker) = marker {
                    query_pairs.append_pair("marker", marker);
                }
                if let Some(max_keys) = max_keys {
                    query_pairs.append_pair("max-keys", &max_keys.to_string());
                }
            }

            Command::ListMultipartUploads {
                prefix,
                delimiter,
                key_marker,
                max_uploads,
            } => {
                let mut query_pairs = url.query_pairs_mut();
                delimiter.map(|d| query_pairs.append_pair("delimiter", d));
                if let Some(prefix) = prefix {
                    query_pairs.append_pair("prefix", prefix);
                }
                if let Some(key_marker) = key_marker {
                    query_pairs.append_pair("key-marker", key_marker);
                }
                if let Some(max_uploads) = max_uploads {
                    query_pairs.append_pair("max-uploads", max_uploads.to_string().as_str());
                }
            }

            Command::PutObjectTagging { .. }
            | Command::GetObjectTagging
            | Command::DeleteObjectTagging => {
                url.query_pairs_mut().append_pair("tagging", "");
            }

            _ => {}
        }

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use tokio::fs;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    async fn test_object_flow() -> Result<(), S3Error> {
        dotenvy::dotenv().ok().unwrap();

        let bucket = Bucket::try_from_env().expect("env vars to be set in .env");

        // we do not use rstest here since the tests seem to interfere with each other on the IO layer
        let file_sizes = vec![
            0,
            1,
            CHUNK_SIZE / 2,
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
        ];

        for file_size in file_sizes {
            println!("test_object_flow with {} bytes", file_size);

            let _ = fs::create_dir_all("test_files").await;
            let file_name_input = format!("test_data_{}", file_size);
            let input_path = format!("test_files/{}", file_name_input);
            let file_name_output = format!("test_data_{}.out", file_size);
            let output_path = format!("test_files/{}", file_name_output);

            // create and write some test data
            let bytes = (0..file_size).into_iter().map(|_| 0u8).collect::<Vec<u8>>();
            fs::write(&input_path, &bytes).await?;

            // upload the file
            let res = bucket.put(&file_name_input, &bytes).await?;
            let status = res.status();
            let body = res.text().await?;
            println!("response body:\n{}", body);
            assert!(status.is_success());

            // give the s3 replication under the hood a second
            tokio::time::sleep(Duration::from_secs(1)).await;

            // GET the file back
            let res = bucket.get(&file_name_input).await?;
            assert!(res.status().is_success());
            let body = res.bytes().await?;
            assert_eq!(body.len(), file_size);
            fs::write(&output_path, body.as_ref()).await?;

            // make sure input and output are the same
            let input_bytes = fs::read(input_path).await?;
            let output_bytes = fs::read(output_path).await?;
            assert_eq!(input_bytes.len(), file_size);
            assert_eq!(input_bytes.len(), output_bytes.len());
            assert_eq!(input_bytes, output_bytes);

            // list bucket content and make sure it shows up
            let list = bucket.list(&bucket.name, None).await?;
            for entry in list.iter() {
                if entry.name == bucket.name {
                    for object in entry.contents.iter() {
                        if object.key == file_name_input {
                            // we found our dummy object, check the size
                            assert_eq!(object.size, file_size as u64);
                            break;
                        }
                    }
                }
            }

            // validate that HEAD is working too
            let res = bucket.head(&file_name_input).await?;
            assert_eq!(res.content_length, Some(file_size as u64));

            if file_size > CHUNK_SIZE / 2 {
                // get only a part of the object back
                let end = CHUNK_SIZE / 2 + 1;
                let res = bucket
                    .get_range(&file_name_input, 0, Some(end as u64))
                    .await?;
                assert!(res.status().is_success());
                let body = res.bytes().await?;
                // the GET range included the end -> 1 additional byte
                assert_eq!(body.len(), end as usize + 1);
            }

            // test internal object copy
            let res = bucket
                .copy_internal(&file_name_input, &file_name_output)
                .await?;
            assert!(res.is_success());

            // GET the new copied object
            let res = bucket.get(&file_name_output).await?;
            assert!(res.status().is_success());
            let body = res.bytes().await?;
            assert_eq!(body.len(), file_size);

            // clean up and delete the test file
            let res = bucket.delete(&file_name_input).await?;
            assert!(res.status().is_success());
            let res = bucket.delete(&file_name_output).await?;
            assert!(res.status().is_success());

            // list bucket content again and make sure its gone
            let list = bucket.list(&bucket.name, None).await?;
            for entry in list.iter() {
                if entry.name == bucket.name {
                    for object in entry.contents.iter() {
                        if object.key == file_name_input {
                            panic!("test file has not been deleted");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    #[traced_test]
    #[tokio::test]
    async fn test_multipart() -> Result<(), S3Error> {
        use futures_util::stream::StreamExt;
        use std::os::unix::fs::MetadataExt;
        use tokio::io::AsyncWriteExt;

        dotenvy::dotenv().ok().unwrap();
        let bucket = Bucket::try_from_env().expect("env vars to be set in .env");

        // we do not use rstest here since the tests seem to interfere with each other on the IO layer
        let file_sizes = vec![
            CHUNK_SIZE - 1,
            CHUNK_SIZE,
            CHUNK_SIZE + 1,
            CHUNK_SIZE * 2,
            CHUNK_SIZE * 3,
            CHUNK_SIZE * 3 + 1,
        ];

        for file_size in file_sizes {
            // create and write some test data
            let _ = fs::create_dir_all("test_files").await;
            let file_name_input = format!("test_data_mp_{}", file_size);
            let input_path = format!("test_files/{}", file_name_input);
            let file_name_output = format!("test_data_mp_{}.out", file_size);
            let output_path = format!("test_files/{}", file_name_output);

            let bytes = (0..file_size).into_iter().map(|_| 0u8).collect::<Vec<u8>>();
            fs::write(&input_path, &bytes).await?;

            // streaming upload
            let mut reader_file = fs::File::open(&input_path).await?;
            let res = bucket
                .put_stream(&mut reader_file, file_name_input.clone())
                .await?;
            assert!(res.status_code < 300);
            assert_eq!(res.uploaded_bytes, file_size);

            // streaming download
            let mut file = fs::File::create(&output_path).await?;

            let res = bucket.get(&file_name_input).await?;
            assert!(res.status().is_success());

            let stream = res.bytes_stream();
            tokio::pin!(stream);
            while let Some(Ok(item)) = stream.next().await {
                file.write(item.as_ref()).await?;
            }
            // flush / sync all possibly left over data
            file.sync_all().await?;

            // make sure the files match
            let f_in = fs::File::open(&input_path).await?;
            let f_out = fs::File::open(&output_path).await?;
            let meta_in = f_in.metadata().await.unwrap();
            let meta_out = f_out.metadata().await.unwrap();
            assert_eq!(meta_in.size(), meta_out.size());
        }

        Ok(())
    }
}
