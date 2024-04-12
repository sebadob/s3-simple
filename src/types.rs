use serde::Deserialize;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub(crate) struct Multipart<'a> {
    part_number: u32,
    upload_id: &'a str,
}

impl<'a> Multipart<'a> {
    pub fn query_string(&self) -> String {
        format!(
            "?partNumber={}&uploadId={}",
            self.part_number, self.upload_id
        )
    }

    pub fn new(part_number: u32, upload_id: &'a str) -> Self {
        Multipart {
            part_number,
            upload_id,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Owner {
    #[serde(rename = "DisplayName")]
    /// Object owner's name.
    pub display_name: Option<String>,
    #[serde(rename = "ID")]
    /// Object owner's ID.
    pub id: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Object {
    #[serde(rename = "LastModified")]
    /// Date and time the object was last modified.
    pub last_modified: String,
    #[serde(rename = "ETag")]
    /// The entity tag is an MD5 hash of the object. The ETag only reflects changes to the
    /// contents of an object, not its metadata.
    pub e_tag: Option<String>,
    #[serde(rename = "StorageClass")]
    /// STANDARD | STANDARD_IA | REDUCED_REDUNDANCY | GLACIER
    pub storage_class: Option<String>,
    #[serde(rename = "Key")]
    /// The object's key
    pub key: String,
    #[serde(rename = "Owner")]
    /// Bucket owner
    pub owner: Option<Owner>,
    #[serde(rename = "Size")]
    /// Size in bytes of the object.
    pub size: u64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct CommonPrefix {
    #[serde(rename = "Prefix")]
    /// Keys that begin with the indicated prefix.
    pub prefix: String,
}

// Taken from https://github.com/rusoto/rusoto
#[derive(Deserialize, Debug, Default, Clone)]
pub struct HeadObjectResult {
    #[serde(rename = "AcceptRanges")]
    /// Indicates that a range of bytes was specified.
    pub accept_ranges: Option<String>,
    #[serde(rename = "CacheControl")]
    /// Specifies caching behavior along the request/reply chain.
    pub cache_control: Option<String>,
    #[serde(rename = "ContentDisposition")]
    /// Specifies presentational information for the object.
    pub content_disposition: Option<String>,
    #[serde(rename = "ContentEncoding")]
    /// Specifies what content encodings have been applied to the object and thus what decoding mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    pub content_encoding: Option<String>,
    #[serde(rename = "ContentLanguage")]
    /// The language the content is in.
    pub content_language: Option<String>,
    #[serde(rename = "ContentLength")]
    /// Size of the body in bytes.
    pub content_length: Option<u64>,
    #[serde(rename = "ContentType")]
    /// A standard MIME type describing the format of the object data.
    pub content_type: Option<String>,
    #[serde(rename = "DeleteMarker")]
    /// Specifies whether the object retrieved was (true) or was not (false) a Delete Marker.
    pub delete_marker: Option<bool>,
    #[serde(rename = "ETag")]
    /// An ETag is an opaque identifier assigned by a web server to a specific version of a resource found at a URL.
    pub e_tag: Option<String>,
    #[serde(rename = "Expiration")]
    /// If the object expiration is configured, the response includes this header. It includes the expiry-date and rule-id key-value pairs providing object expiration information.
    /// The value of the rule-id is URL encoded.
    pub expiration: Option<String>,
    #[serde(rename = "Expires")]
    /// The date and time at which the object is no longer cacheable.
    pub expires: Option<String>,
    #[serde(rename = "LastModified")]
    /// Last modified date of the object
    pub last_modified: Option<String>,
    #[serde(rename = "Metadata", default)]
    /// A map of metadata to store with the object in S3.
    pub metadata: Option<::std::collections::HashMap<String, String>>,
    #[serde(rename = "MissingMeta")]
    /// This is set to the number of metadata entries not returned in x-amz-meta headers. This can happen if you create metadata using an API like SOAP that supports more flexible metadata than
    /// the REST API. For example, using SOAP, you can create metadata whose values are not legal HTTP headers.
    pub missing_meta: Option<i64>,
    #[serde(rename = "ObjectLockLegalHoldStatus")]
    /// Specifies whether a legal hold is in effect for this object. This header is only returned if the requester has the s3:GetObjectLegalHold permission.
    /// This header is not returned if the specified version of this object has never had a legal hold applied.
    pub object_lock_legal_hold_status: Option<String>,
    #[serde(rename = "ObjectLockMode")]
    /// The Object Lock mode, if any, that's in effect for this object.
    pub object_lock_mode: Option<String>,
    #[serde(rename = "ObjectLockRetainUntilDate")]
    /// The date and time when the Object Lock retention period expires.
    /// This header is only returned if the requester has the s3:GetObjectRetention permission.
    pub object_lock_retain_until_date: Option<String>,
    #[serde(rename = "PartsCount")]
    /// The count of parts this object has.
    pub parts_count: Option<i64>,
    #[serde(rename = "ReplicationStatus")]
    /// If your request involves a bucket that is either a source or destination in a replication rule.
    pub replication_status: Option<String>,
    #[serde(rename = "RequestCharged")]
    pub request_charged: Option<String>,
    #[serde(rename = "Restore")]
    /// If the object is an archived object (an object whose storage class is GLACIER), the response includes this header if either the archive restoration is in progress or an archive copy is already restored.
    /// If an archive copy is already restored, the header value indicates when Amazon S3 is scheduled to delete the object copy.
    pub restore: Option<String>,
    #[serde(rename = "SseCustomerAlgorithm")]
    /// If server-side encryption with a customer-provided encryption key was requested, the response will include this header confirming the encryption algorithm used.
    pub sse_customer_algorithm: Option<String>,
    #[serde(rename = "SseCustomerKeyMd5")]
    /// If server-side encryption with a customer-provided encryption key was requested, the response will include this header to provide round-trip message integrity verification of the customer-provided encryption key.
    pub sse_customer_key_md5: Option<String>,
    #[serde(rename = "SsekmsKeyId")]
    /// If present, specifies the ID of the AWS Key Management Service (AWS KMS) symmetric customer managed customer master key (CMK) that was used for the object.
    pub ssekms_key_id: Option<String>,
    #[serde(rename = "ServerSideEncryption")]
    /// If the object is stored using server-side encryption either with an AWS KMS customer master key (CMK) or an Amazon S3-managed encryption key,
    /// The response includes this header with the value of the server-side encryption algorithm used when storing this object in Amazon S3 (for example, AES256, aws:kms).
    pub server_side_encryption: Option<String>,
    #[serde(rename = "StorageClass")]
    /// Provides storage class information of the object. Amazon S3 returns this header for all objects except for S3 Standard storage class objects.
    pub storage_class: Option<String>,
    #[serde(rename = "VersionId")]
    /// Version of the object.
    pub version_id: Option<String>,
    #[serde(rename = "WebsiteRedirectLocation")]
    /// If the bucket is configured as a website, redirects requests for this object to another object in the same bucket or to an external URL. Amazon S3 stores the value of this header in the object metadata.
    pub website_redirect_location: Option<String>,
}

trait GetAndConvertHeaders {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T>;
    fn get_string(&self, header: &str) -> Option<String>;
}

impl GetAndConvertHeaders for http::header::HeaderMap {
    fn get_and_convert<T: FromStr>(&self, header: &str) -> Option<T> {
        self.get(header)?.to_str().ok()?.parse::<T>().ok()
    }
    fn get_string(&self, header: &str) -> Option<String> {
        Some(self.get(header)?.to_str().ok()?.to_owned())
    }
}

impl From<&http::HeaderMap> for HeadObjectResult {
    fn from(headers: &http::HeaderMap) -> Self {
        let mut result = HeadObjectResult {
            accept_ranges: headers.get_string("accept-ranges"),
            cache_control: headers.get_string("Cache-Control"),
            content_disposition: headers.get_string("Content-Disposition"),
            content_encoding: headers.get_string("Content-Encoding"),
            content_language: headers.get_string("Content-Language"),
            content_length: headers.get_and_convert("Content-Length"),
            content_type: headers.get_string("Content-Type"),
            delete_marker: headers.get_and_convert("x-amz-delete-marker"),
            e_tag: headers.get_string("ETag"),
            expiration: headers.get_string("x-amz-expiration"),
            expires: headers.get_string("Expires"),
            last_modified: headers.get_string("Last-Modified"),
            ..Default::default()
        };
        let mut values = ::std::collections::HashMap::new();
        for (key, value) in headers.iter() {
            if key.as_str().starts_with("x-amz-meta-") {
                if let Ok(value) = value.to_str() {
                    values.insert(
                        key.as_str()["x-amz-meta-".len()..].to_owned(),
                        value.to_owned(),
                    );
                }
            }
        }
        result.metadata = Some(values);
        result.missing_meta = headers.get_and_convert("x-amz-missing-meta");
        result.object_lock_legal_hold_status = headers.get_string("x-amz-object-lock-legal-hold");
        result.object_lock_mode = headers.get_string("x-amz-object-lock-mode");
        result.object_lock_retain_until_date =
            headers.get_string("x-amz-object-lock-retain-until-date");
        result.parts_count = headers.get_and_convert("x-amz-mp-parts-count");
        result.replication_status = headers.get_string("x-amz-replication-status");
        result.request_charged = headers.get_string("x-amz-request-charged");
        result.restore = headers.get_string("x-amz-restore");
        result.sse_customer_algorithm =
            headers.get_string("x-amz-server-side-encryption-customer-algorithm");
        result.sse_customer_key_md5 =
            headers.get_string("x-amz-server-side-encryption-customer-key-MD5");
        result.ssekms_key_id = headers.get_string("x-amz-server-side-encryption-aws-kms-key-id");
        result.server_side_encryption = headers.get_string("x-amz-server-side-encryption");
        result.storage_class = headers.get_string("x-amz-storage-class");
        result.version_id = headers.get_string("x-amz-version-id");
        result.website_redirect_location = headers.get_string("x-amz-website-redirect-location");
        result
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct ListBucketResult {
    #[serde(rename = "Name")]
    /// Name of the bucket.
    pub name: String,
    #[serde(rename = "Delimiter")]
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    #[serde(rename = "MaxKeys")]
    /// Sets the maximum number of keys returned in the response body.
    pub max_keys: Option<i32>,
    #[serde(rename = "Prefix")]
    /// Limits the response to keys that begin with the specified prefix.
    pub prefix: Option<String>,
    #[serde(rename = "ContinuationToken")] // for ListObjectsV2 request
    #[serde(alias = "Marker")] // for ListObjects request
    /// Indicates where in the bucket listing begins. It is included in the response if
    /// it was sent with the request.
    pub continuation_token: Option<String>,
    #[serde(rename = "EncodingType")]
    /// Specifies the encoding method to used
    pub encoding_type: Option<String>,
    #[serde(default, rename = "IsTruncated")]
    ///  Specifies whether (true) or not (false) all of the results were returned.
    ///  If the number of results exceeds that specified by MaxKeys, all of the results
    ///  might not be returned.

    /// When the response is truncated (that is, the IsTruncated element value in the response
    /// is true), you can use the key name in in 'next_continuation_token' as a marker in the
    /// subsequent request to get next set of objects. Amazon S3 lists objects in UTF-8 character
    /// encoding in lexicographical order.
    pub is_truncated: bool,
    #[serde(rename = "NextContinuationToken", default)] // for ListObjectsV2 request
    #[serde(alias = "NextMarker")] // for ListObjects request
    pub next_continuation_token: Option<String>,
    #[serde(rename = "Contents", default)]
    /// Metadata about each object returned.
    pub contents: Vec<Object>,
    #[serde(rename = "CommonPrefixes", default)]
    /// All of the keys rolled up into a common prefix count as a single return when
    /// calculating the number of returns.
    pub common_prefixes: Option<Vec<CommonPrefix>>,
}

#[derive(Deserialize, Debug)]
pub(crate) struct InitiateMultipartUploadResponse {
    #[serde(rename = "Bucket")]
    _bucket: String,
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

#[derive(Debug)]
pub struct PutStreamResponse {
    pub status_code: u16,
    pub uploaded_bytes: usize,
}
